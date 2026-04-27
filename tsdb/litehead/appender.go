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

// appender 是 LiteHead 对外暴露的 storage.Appender 实现。
type appender struct {
	head *Head

	// pending* 系列字段存放本次事务收集到的数据，Commit 时一次性写 WAL。
	pendingSeries  []record.RefSeries
	pendingSamples []record.RefSample
	// sampleSeries[i] 是 pendingSamples[i] 对应的 *memSeries，
	// Commit 阶段用它更新 lastTs。
	sampleSeries []*memSeries
}

// GetRef 允许调用方在无 ref 时先做一次 labels -> ref 的查找。
// 只要 hash 和 lset 能在 hashIndex 命中，就返回 (ref, lset)；
// 未命中时返回 (0, EmptyLabels)。
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

	// 对齐标准 Head：任何落在 minValidTime 之前的样本都不能再写入。
	// flush 开始后会先推进这个边界，避免样本错过 block 又被后续 truncate 清掉。
	if t < a.head.appendableMinValidTime() {
		return 0, storage.ErrOutOfBounds
	}

	s.mu.Lock()
	// 乱序样本：直接拒绝，保持与原生 Head in-order 分支一致。
	// 检查两个来源：
	//   1. 已 commit 的 lastTs（跨 batch 的顺序）；
	//   2. 当前 batch 写进 open chunk 的 openMaxT（batch 内的顺序）。
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

// AppendExemplar：LiteHead 目前不持久化 exemplar（Mimir ingester 也
// 只关心 samples 的复用），为了兼容接口直接忽略并返回 0。
// TODO: 如需完整 exemplar 支持，按 WAL 记录类型写入并在 replay 时跳过即可。
func (a *appender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, nil
}

// AppendHistogram / UpdateMetadata：
//
// 当前版本仅关注 float in-order 主路径。为了让 LiteHead 可以直接代替
// tsdb.Head 注入到 storage.Appendable 里，先把这些方法实现成“吸收式”
// 占位（不 panic、也不保存数据）。TODO: 逐步补齐 histogram 与 metadata。
func (a *appender) AppendHistogram(storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *appender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, nil
}

// Commit 把本次 batch 的数据持久化到 WAL，并更新 lastTs 与 Head 时间窗。
func (a *appender) Commit() error {
	defer a.reset()

	if err := a.logWAL(); err != nil {
		return err
	}

	// 更新 series 的 lastTs 与 Head 时间窗口。
	// 注意：这里必须在 WAL 落盘后再更新，避免崩溃时 WAL 缺少对应样本却已
	// 经更新了 lastTs。
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

// Rollback 丢弃本批次的样本，但保留已经分配的新 series（必须写入 WAL，
// 否则后续 ref 会指向 WAL 中不存在的 series）。
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
		// 热路径：直接按 ref 查。
		if s := a.head.refTab.get(chunks.HeadSeriesRef(ref)); s != nil {
			return s, nil
		}
		// ref 不存在，回退到冷路径。
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
	s := a.head.createSeries(hash, lset)
	a.pendingSeries = append(a.pendingSeries, record.RefSeries{
		Ref:    s.ref,
		Labels: lset,
	})
	a.head.metrics.seriesCreated.Inc()
	return s, nil
}

// ensureOpenChunk 确保 s 有一个可写的 open chunk。返回是否新建了一个 chunk。
// 调用方必须已经持有 s.mu。
func (a *appender) ensureOpenChunk(s *memSeries, t int64, enc chunkenc.Encoding) bool {
	if s.openChunk != nil {
		return false
	}
	return a.cutNewChunkLocked(s, t, enc)
}

// cutNewChunkLocked 切出一个新的 open chunk。必须持有 s.mu。
// 返回值表示是否新建了 chunk，供 caller 统计 metrics。
func (a *appender) cutNewChunkLocked(s *memSeries, t int64, enc chunkenc.Encoding) bool {
	var chk chunkenc.Chunk
	if chunkenc.IsValidEncoding(enc) {
		var err error
		chk, err = chunkenc.NewEmptyChunk(enc)
		if err != nil {
			// NewEmptyChunk 只会在 encoding 非法时返回错误，前面已校验过，
			// 这里理论上不可能走到。保守地走 XOR 兜底。
			chk = chunkenc.NewXORChunk()
		}
	} else {
		chk = chunkenc.NewXORChunk()
	}
	app, err := chk.Appender()
	if err != nil {
		// Appender() 的错误同样罕见；兜底用 XOR。
		chk = chunkenc.NewXORChunk()
		app, _ = chk.Appender()
	}
	s.openChunk = chk
	s.openApp = app
	s.openMinT = t
	s.openMaxT = math.MinInt64
	s.nextAt = rangeForTimestamp(t, a.head.opts.ChunkRange)
	return true
}

// maybeCutChunk 判断是否需要切出新的 chunk，并把老的 open chunk spill 到磁盘。
// 必须持有 s.mu。
func (a *appender) maybeCutChunk(s *memSeries, t int64, enc chunkenc.Encoding) error {
	c := s.openChunk
	if c == nil {
		return nil
	}

	numSamples := c.NumSamples()

	// 与原生 Head 对齐：chunk 字节过大时先切（留 19 字节余量）。
	const maxBytesPerXORChunk = chunkenc.MaxBytesPerXORChunk - 19
	tooBig := enc == chunkenc.EncXOR && len(c.Bytes()) > maxBytesPerXORChunk

	// 到达 chunkRange 边界或样本数超过 2x 上限时也切。
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

// sealAndSpillLocked 把当前 open chunk 封口并写到 ChunkDiskMapper。
// 封口后 s.openChunk 变为 nil，等待下一条样本再懒分配。
// 必须持有 s.mu。
//
// 当 sealed[] 已经占满时，这里 *绝不* 丢弃数据：会临时释放 s.mu，触发
// 一次同步的全库 flushBlocking 把窗口内 sealed 数据落成 block，然后
// 重新拿回 s.mu 继续封装当前 open chunk。
func (a *appender) sealAndSpillLocked(s *memSeries) error {
	c := s.openChunk
	if c == nil || c.NumSamples() == 0 {
		// 没数据，直接丢弃（空 chunk 本身就不含任何样本，丢弃是安全的）。
		s.openChunk = nil
		s.openApp = nil
		return nil
	}

	// sealed 数组达到上限：同步触发一次全库 flushBlocking。
	//
	// 为什么要释放 s.mu：flushBlocking 会从 WAL 读样本并写入临时 Head，
	// 本身不会再回头获取 s.mu；但在它跑起来之前，会先拿 db.flushMtx。
	// 为了避免其它 goroutine 在"持有 s.mu 的同时等 flushMtx"引起锁序
	// 倒转，这里先释放 s.mu，flush 完再重新拿回来。
	//
	// 注意：释放 s.mu 后，openChunk / sealed[] / openApp 可能被其它
	// appender 改动，所以 flush 回来后必须重新做一次状态检查。
	for s.mmappedChunksCount >= maxMmappedChunksPerSeries {
		a.head.metrics.mmappedChunksForcedFlush.Inc()
		// 在释放 s.mu 之前，先把当前 series 已知的最大时间更新到 db.maxTime，
		// 否则 flushBlocking 看到的 MaxTime 不包含当前 batch 的 spill，
		// 导致 flush 范围不够大、mmapped chunks 清不掉。
		if s.openMaxT != math.MinInt64 {
			a.head.updateMinMaxTime(s.openMaxT)
		}
		for i := uint8(0); i < s.mmappedChunksCount; i++ {
			a.head.updateMinMaxTime(s.mmappedChunks[i].maxTime)
		}
		s.mu.Unlock()
		err := a.head.flushBlocking()
		s.mu.Lock()
		if err != nil {
			return errors.Wrap(err, "forced flush on mmapped chunks overflow")
		}
		// flushBlocking 内部的 truncateMmapped 已经清理了 maxTime <= flushMaxt
		// 的条目。如果 mmappedChunksCount 仍然满，做一次精确清理作为保底。
		if s.mmappedChunksCount >= maxMmappedChunksPerSeries {
			flushedMaxt := a.head.MaxTime()
			n := uint8(0)
			for i := uint8(0); i < s.mmappedChunksCount; i++ {
				if s.mmappedChunks[i].maxTime > flushedMaxt {
					s.mmappedChunks[n] = s.mmappedChunks[i]
					n++
				}
			}
			for i := n; i < s.mmappedChunksCount; i++ {
				s.mmappedChunks[i] = mmappedChunk{}
			}
			s.mmappedChunksCount = n
			break
		}
	}

	// 重新取 openChunk：flushBlocking 期间可能已被其它 appender 切走。
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

	// WriteChunk 异步：用回调记录错误，但我们不在热路径等待。
	chkRef := a.head.chunkDiskMapper.WriteChunk(s.ref, mint, maxt, c, false, nil)

	s.mmappedChunks[s.mmappedChunksCount] = mmappedChunk{
		ref:        chkRef,
		minTime:    mint,
		maxTime:    maxt,
		encoding:   c.Encoding(),
		numSamples: uint16(c.NumSamples()),
	}
	s.mmappedChunksCount++
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
	a.pendingSeries = a.pendingSeries[:0]
	a.pendingSamples = a.pendingSamples[:0]
	a.sampleSeries = a.sampleSeries[:0]
	a.head.appenderPool.Put(a)
}
