package litehead

import (
	"fmt"
	"math"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
)

// appender 实现 storage.Appender。
type appender struct {
	head *Head

	pendingSeries  []record.RefSeries
	pendingSamples []record.RefSample
	sampleSeries   []*memSeries
}

// GetRef 查找 labels 对应的 ref。
func (a *appender) GetRef(lset labels.Labels, hash uint64) (storage.SeriesRef, labels.Labels) {
	if ref, ok := a.head.hashIdx.get(hash, lset, a.head.labelCat); ok {
		return storage.SeriesRef(ref), lset
	}
	return 0, labels.EmptyLabels()
}

// ----- 样本写入 -----

func (a *appender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	s, err := a.resolveSeries(ref, lset)
	if err != nil {
		return 0, err
	}

	if t < a.head.appendableMinValidTime() {
		return 0, storage.ErrOutOfBounds
	}

	s.mu.Lock()
	if (s.lastTs != math.MinInt64 && t <= s.lastTs) ||
		(s.openChunk != nil && s.openChunk.NumSamples() > 0 && t <= s.openMaxT) {
		s.mu.Unlock()
		a.head.metrics.outOfOrderSamples.Inc()
		return 0, storage.ErrOutOfOrderSample
	}

	// open chunk 懒分配；必要时切新 chunk。
	created := a.ensureOpenChunk(s, t, chunkenc.EncXOR)
	if created {
		a.head.metrics.chunksCreated.Inc()
	}

	if err := a.maybeCutChunk(s, t, chunkenc.EncXOR); err != nil {
		s.mu.Unlock()
		return 0, err
	}

	s.openApp.Append(t, v)
	if t > s.openMaxT {
		s.openMaxT = t
	}
	s.mu.Unlock()

	a.pendingSamples = append(a.pendingSamples, record.RefSample{Ref: s.ref, T: t, V: v})
	a.sampleSeries = append(a.sampleSeries, s)
	return storage.SeriesRef(s.ref), nil
}

// ErrUnsupportedWriteType 表示该写入类型未被 litehead 支持。
// litehead 当前仅支持 in-order float samples；exemplar/histogram/metadata 写入
// 都会显式返回该错误，避免调用方误以为写入成功。
var ErrUnsupportedWriteType = errors.New("litehead: exemplar/histogram/metadata writes are not supported")

// AppendExemplar 不支持 exemplar 写入，显式返回 ErrUnsupportedWriteType。
func (a *appender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, ErrUnsupportedWriteType
}

// AppendHistogram 不支持 histogram 写入，显式返回 ErrUnsupportedWriteType。
func (a *appender) AppendHistogram(storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, ErrUnsupportedWriteType
}

// UpdateMetadata 不支持 metadata 写入，显式返回 ErrUnsupportedWriteType。
func (a *appender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, ErrUnsupportedWriteType
}

// Commit 持久化到 WAL 并更新 lastTs。
func (a *appender) Commit() error {
	defer a.reset()

	if err := a.logWAL(); err != nil {
		return err
	}

	for i, s := range a.sampleSeries {
		sam := a.pendingSamples[i]
		s.mu.Lock()
		if sam.T > s.lastTs {
			s.lastTs = sam.T
		}
		s.mu.Unlock()
		a.head.updateMinMaxTime(sam.T)
		a.head.metrics.samplesAppended.Inc()
	}
	return nil
}

// Rollback 丢弃样本，但保留已分配的新 series（须写入 WAL）。
func (a *appender) Rollback() error {
	defer a.reset()
	if len(a.pendingSeries) == 0 {
		return nil
	}
	return a.logOnlyPendingSeries()
}

// ----- 内部辅助 -----

func (a *appender) resolveSeries(ref storage.SeriesRef, lset labels.Labels) (*memSeries, error) {
	if ref != 0 {
		if s := a.head.refTab.get(chunks.HeadSeriesRef(ref)); s != nil {
			return s, nil
		}
	}

	if lset.IsEmpty() {
		return nil, errors.New("litehead: append with empty labels and unknown ref")
	}
	lset = lset.WithoutEmpty()
	if lbl, dup := lset.HasDuplicateLabelNames(); dup {
		return nil, fmt.Errorf("litehead: duplicate label name %q", lbl)
	}

	hash := lset.Hash()
	if foundRef, ok := a.head.hashIdx.get(hash, lset, a.head.labelCat); ok {
		if s := a.head.refTab.get(foundRef); s != nil {
			return s, nil
		}
	}

	// 真正新 series：创建并记录到 pending 里。
	s, err := a.head.createSeries(hash, lset)
	if err != nil {
		return nil, err
	}
	a.pendingSeries = append(a.pendingSeries, record.RefSeries{
		Ref:    s.ref,
		Labels: lset,
	})
	a.head.metrics.seriesCreated.Inc()
	return s, nil
}

// ensureOpenChunk 确保 s 有可写的 open chunk。调用方须持有 s.mu。
func (a *appender) ensureOpenChunk(s *memSeries, t int64, enc chunkenc.Encoding) bool {
	if s.openChunk != nil {
		return false
	}
	return a.cutNewChunkLocked(s, t, enc)
}

// cutNewChunkLocked 切出新的 open chunk。须持有 s.mu。
func (a *appender) cutNewChunkLocked(s *memSeries, t int64, enc chunkenc.Encoding) bool {
	var chk chunkenc.Chunk
	if chunkenc.IsValidEncoding(enc) {
		var err error
		chk, err = chunkenc.NewEmptyChunk(enc)
		if err != nil {
			chk = chunkenc.NewXORChunk()
		}
	} else {
		chk = chunkenc.NewXORChunk()
	}
	app, err := chk.Appender()
	if err != nil {
		chk = chunkenc.NewXORChunk()
		app, _ = chk.Appender()
	}
	s.openChunk = chk
	s.openApp = app
	s.openMinT = t
	s.openMaxT = math.MinInt64
	s.nextAt = rangeForTimestamp(t, a.head.chunkRange.Load())
	return true
}

// maybeCutChunk 按需切 chunk 并 spill 到磁盘。须持有 s.mu。
func (a *appender) maybeCutChunk(s *memSeries, t int64, enc chunkenc.Encoding) error {
	c := s.openChunk
	if c == nil {
		return nil
	}

	numSamples := c.NumSamples()

	const maxBytesPerXORChunk = chunkenc.MaxBytesPerXORChunk - 19
	tooBig := enc == chunkenc.EncXOR && len(c.Bytes()) > maxBytesPerXORChunk
	needCut := tooBig || t >= s.nextAt || numSamples >= a.head.opts.SamplesPerChunk*2 || c.Encoding() != enc

	if !needCut {
		return nil
	}

	if err := a.sealAndSpillLocked(s); err != nil {
		return err
	}
	a.cutNewChunkLocked(s, t, enc)
	a.head.metrics.chunksCreated.Inc()
	return nil
}

// sealAndSpillLocked 封口当前 open chunk 并写到 ChunkDiskMapper。须持有 s.mu。
//
// sealed chunks 上限采用 "soft watermark + hard limit" 两级策略（PR-4）：
//   - sealed 数刚好等于 soft 阈值时：仅递增一次 soft flush hit 计数，**不**做 flush；
//   - sealed 数 >= hard 阈值时：释放 s.mu，同步 flushBlocking 作为兜底；
//   - 配置由 Options.SoftFlushSealedChunks / Options.ForcedFlushSealedChunks 控制，
//     default 见 series.go。
//
// 设计意图：让 forced flush 从"每次 sealed 满就触发"降级为"极端罕见的兜底路径"，
// soft 指标成为提前一步的观测信号。
func (a *appender) sealAndSpillLocked(s *memSeries) error {
	c := s.openChunk
	if c == nil || c.NumSamples() == 0 {
		s.openChunk = nil
		s.openApp = nil
		return nil
	}

	hardLimit := a.head.opts.ForcedFlushSealedChunks
	softLimit := a.head.opts.SoftFlushSealedChunks

	// 软阈值：只告警，不 flush。只在"刚好跨过"那一次 +1，避免每次 append 都打点。
	// sealedLen() 此刻是尚未 append 本次 sealed chunk 的数量；+1 即为本次追加后的值。
	if softLimit > 0 && s.sealedLen()+1 == softLimit {
		a.head.metrics.mmappedChunksSoftFlushHits.Inc()
	}

	// 硬上限：释放 s.mu，同步 flush，再重新获取。
	for s.sealedLen() >= hardLimit {
		a.head.metrics.mmappedChunksForcedFlush.Inc()
		// 更新 maxTime 让 flush 覆盖到当前数据。
		if s.openMaxT != math.MinInt64 {
			a.head.updateMinMaxTime(s.openMaxT)
		}
		s.forEachSealed(func(mc mmappedChunk) {
			a.head.updateMinMaxTime(mc.maxTime)
		})
		s.mu.Unlock()
		err := a.head.flushBlocking()
		s.mu.Lock()
		if err != nil {
			return errors.Wrap(err, "forced flush on mmapped chunks overflow")
		}
		// 保底清理：如果 flush 没能完全清空 sealed，手动清理。
		if s.sealedLen() >= hardLimit {
			flushedMaxt := a.head.MaxTime()
			s.retainSealedAfter(flushedMaxt, nil)
			break
		}
	}

	// flush 期间 openChunk 可能已被其它 appender 切走，重新检查。
	c = s.openChunk
	if c == nil || c.NumSamples() == 0 {
		s.openChunk = nil
		s.openApp = nil
		return nil
	}

	mint := s.openMinT
	maxt := s.openMaxT
	if maxt < mint {
		maxt = mint
	}

	chkRef := a.head.chunkDiskMapper.WriteChunk(s.ref, mint, maxt, c, false, nil)

	s.appendSealed(mmappedChunk{
		ref:        chkRef,
		minTime:    mint,
		maxTime:    maxt,
		numSamples: uint16(c.NumSamples()),
	})
	s.openChunk = nil
	s.openApp = nil
	a.head.metrics.chunksSealed.Inc()
	return nil
}

// logWAL 一次性把 pending 的 series 和 samples 写入 WAL。
func (a *appender) logWAL() error {
	if len(a.pendingSeries) == 0 && len(a.pendingSamples) == 0 {
		return nil
	}

	var enc record.Encoder
	pbuf := a.head.bufPool.Get().(*[]byte)
	buf := (*pbuf)[:0]
	defer func() {
		*pbuf = buf[:0]
		a.head.bufPool.Put(pbuf)
	}()

	if len(a.pendingSeries) > 0 {
		buf = enc.Series(a.pendingSeries, buf)
		if err := a.head.wal.Log(buf); err != nil {
			return errors.Wrap(err, "log WAL series")
		}
		buf = buf[:0]
	}
	if len(a.pendingSamples) > 0 {
		buf = enc.Samples(a.pendingSamples, buf)
		if err := a.head.wal.Log(buf); err != nil {
			return errors.Wrap(err, "log WAL samples")
		}
		buf = buf[:0]
	}
	return nil
}

// logOnlyPendingSeries 仅写 pendingSeries。
func (a *appender) logOnlyPendingSeries() error {
	if len(a.pendingSeries) == 0 {
		return nil
	}
	var enc record.Encoder
	pbuf := a.head.bufPool.Get().(*[]byte)
	buf := (*pbuf)[:0]
	defer func() {
		*pbuf = buf[:0]
		a.head.bufPool.Put(pbuf)
	}()

	buf = enc.Series(a.pendingSeries, buf)
	return a.head.wal.Log(buf)
}

// reset 清理 appender 状态并放回 pool。
func (a *appender) reset() {
	head := a.head
	a.pendingSeries = a.pendingSeries[:0]
	a.pendingSamples = a.pendingSamples[:0]
	a.sampleSeries = a.sampleSeries[:0]
	head.appenderMtx.RUnlock()
	head.appenderPool.Put(a)
}
