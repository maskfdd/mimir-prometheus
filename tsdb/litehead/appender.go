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
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
)

// errAppender 是一个永远失败的 appender，返回给调用方以在 Head 未 Init 时
// 安全拒绝写入；所有 Append* 和 UpdateMetadata 都返回预置 err，Commit/Rollback
// 返回同一 err。注意：调用方按 storage.Appender 契约最终会 Commit 或 Rollback，
// 这里两者都返回 err 而不 panic，避免调用方崩溃。
type errAppender struct{ err error }

func (e errAppender) Append(storage.SeriesRef, labels.Labels, int64, float64) (storage.SeriesRef, error) {
	return 0, e.err
}
func (e errAppender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, e.err
}
func (e errAppender) AppendHistogram(storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, e.err
}
func (e errAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, e.err
}
func (e errAppender) GetRef(labels.Labels, uint64) (storage.SeriesRef, labels.Labels) {
	return 0, labels.EmptyLabels()
}
func (e errAppender) Commit() error   { return e.err }
func (e errAppender) Rollback() error { return e.err }

// appender 实现 storage.Appender。
//
// 写入语义（A1 修复后）：
//
//	Append 只做**只读**乱序预检并把样本放入 pending buffer；
//	实际写入 open chunk、更新 s.openMaxT / s.lastTs、以及必要的
//	sealAndSpillLocked 都发生在 Commit 阶段。
//
// 这样 Rollback 能真正撤销：pending 样本会被丢弃，memSeries 上的状态
// 与样本未提交前保持一致。
//
// 同一个 appender 内的批内乱序预检（同一 *memSeries 上多次 Append）通过
// batchSeriesMaxT 维护：它只记本批次内已登记的最大 t，Commit/Rollback
// 后清空。跨 appender 并发写同一 series 的真实落盘检查，见 Commit 里的
// "最终防线"分支。
type appender struct {
	head *Head

	pendingSeries  []record.RefSeries
	pendingSamples []record.RefSample
	sampleSeries   []*memSeries

	// batchGen 是本 appender 的批次代号，每次 Commit/Rollback 递增。
	// 配合 memSeries.batchGen/batchMaxT 实现 O(1) 批内乱序检测，
	// 替代之前的 map[*memSeries]int64，避免 map 分配和 delete 循环开销。
	batchGen uint64
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

	// 只读预检 + 批内乱序检测：一次持锁完成所有检查和 batchGen 登记。
	// batchGen/batchMaxT 存储在 memSeries 上，替代之前 appender 上的
	// map[*memSeries]int64，实现 O(1) 批内乱序检测，无 map 分配开销。
	s.mu.Lock()
	lastTs := s.lastTs
	openMaxT := s.openMaxT
	openHasSamples := s.openChunk != nil && s.openChunk.NumSamples() > 0

	if (lastTs != math.MinInt64 && t <= lastTs) ||
		(openHasSamples && t <= openMaxT) ||
		(s.batchGen == a.batchGen && t <= s.batchMaxT) {
		s.mu.Unlock()
		a.head.metrics.outOfOrderSamples.Inc()
		return 0, storage.ErrOutOfOrderSample
	}
	// 登记本批次的 batchGen 和 batchMaxT。
	s.batchGen = a.batchGen
	s.batchMaxT = t
	s.mu.Unlock()

	// 登记到 pending：真正的 open chunk 写入延后到 Commit。
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

// Commit 持久化到 WAL 并把 pending 样本真正写入 open chunk / 更新 lastTs。
//
// 顺序约束：
//  1. 获取 appenderMtx.RLock，阻塞 SelfCompact 抢占内存状态；
//  2. logWAL()：若 WAL 失败，pending 样本不会落到 chunk，也不会更新
//     series 状态——等价于 Rollback，调用方看到 error 即知未持久化。
//  3. 按 pending 顺序重放样本到 open chunk。每条样本重新在 s.mu 下走
//     ensureOpenChunk / maybeCutChunk / openApp.Append，并原子更新
//     openMaxT / lastTs。
//
// 错误处理：commitSample 可能在 forced flush 等路径上返回 IO 错误。
// 为保持 "WAL 已写 -> 内存状态尽可能同步" 的一致性，循环**不中断**，
// 把所有 error 聚合为 multi-error 返回。后续重启时 replay 会用 WAL
// 中的 samples 重新推高各 series 的 lastTs（幂等），不会造成脏 block。
//
// 注意：Append 阶段的预检是乐观的（读 s.lastTs / s.openMaxT 不加写锁）。
// 跨 appender 并发场景下，另一个 appender 可能在本 Commit 执行期间先提交了
// 更大或等于 t 的样本；此时 Commit 在真写入前要做"最终防线"二次校验，
// 把乱序样本丢弃并计数，避免 XOR chunk 出现非单调 t 序列。
// 二次校验失败的样本已经写进了 WAL，但 WAL replay 只更新 lastTs（幂等），
// 不会产生脏 block。
func (a *appender) Commit() error {
	a.head.appenderMtx.RLock()
	defer func() {
		a.head.appenderMtx.RUnlock()
		a.reset()
	}()

	if err := a.logWAL(); err != nil {
		return err
	}

	// 按"连续同 series"分组一次性持锁处理：Mimir push / remote_write 场景里
	// 同一 appender 常包含同 series 多条样本，逐样本 Lock/Unlock 会在 s.mu
	// 上产生大量无意义抖动。分组后每组只走一次 Lock/Unlock，其他路径语义
	// 完全一致（最终防线、ensureOpenChunk / maybeCutChunk、openMaxT/lastTs
	// 更新、metrics 均逐样本执行）。
	var errs = tsdb_errors.NewMulti()
	n := len(a.sampleSeries)
	i := 0
	for i < n {
		// 找到 [i, j) 这段连续同 series。
		s := a.sampleSeries[i]
		j := i + 1
		for j < n && a.sampleSeries[j] == s {
			j++
		}
		if err := a.commitSampleRun(s, a.pendingSamples[i:j]); err != nil {
			errs.Add(err)
			// 继续处理后续样本：WAL 是权威来源，内存 state 尽量同步。
		}
		i = j
	}
	return errs.Err()
}

// commitSampleRun 在一次 s.mu 持锁区间内处理同一 series 的连续样本。
// 调用方**不**持 s.mu；内部自取并释放。samples 必须非空且全部属于 s。
//
// 语义与之前逐样本的 commitSample 等价：
//   - 每条样本独立做最终防线校验（t <= s.lastTs / t <= s.openMaxT），失败计数并跳过；
//   - 每条样本独立走 ensureOpenChunk / maybeCutChunk，因此中途可能跨 chunk 切换；
//   - maybeCutChunk 触达 hard limit 时会 Unlock/flushBlocking/Lock（原有行为），
//     本函数通过返回 err 中断当前 run，把错误上交给 Commit 循环聚合；
//   - samplesAppended 指标按 run 累加后一次性 Add；
//   - updateMinMaxTime 在 run 内实际落盘的 t 单调递增，只需对"首个落盘 t"
//     推 minTime、"末尾落盘 t"推 maxTime 各一次，结果与逐样本等价但省掉
//     N-1 次 CAS loop。
func (a *appender) commitSampleRun(s *memSeries, samples []record.RefSample) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var appended float64
	var firstOK, lastOK int64
	firstOK = math.MaxInt64 // 哨兵：尚未有成功样本。

	for _, sam := range samples {
		t, v := sam.T, sam.V

		// 最终防线：在真写入前再次校验单调性。可能被其它 appender 抢先提交
		// 更大的 t，或者本 run 之前已处理的样本刚刚推高了 openMaxT/lastTs。
		if s.lastTs != math.MinInt64 && t <= s.lastTs {
			a.head.metrics.outOfOrderSamples.Inc()
			continue
		}
		if s.openChunk != nil && s.openChunk.NumSamples() > 0 && t <= s.openMaxT {
			a.head.metrics.outOfOrderSamples.Inc()
			continue
		}
		// 对 inline 样本也做乱序检查。
		if s.hasInlineSamples() && t <= s.inlineTs[s.inlineN-1] {
			a.head.metrics.outOfOrderSamples.Inc()
			continue
		}

		// P2 优化：inline 样本缓冲路径。
		// 如果还没有 open chunk 且 inline 缓冲区未满，先存 inline，不分配 chunk。
		// 仅当 SamplesPerChunk > maxInlineSamples 时启用：极端小 SamplesPerChunk 配置
		// 下，inline 回填后立即触发 maybeCutChunk 会改变 seal 节奏，影响 forced flush
		// 等依赖精确 seal 计数的逻辑。
		if s.openChunk == nil && s.inlineN < maxInlineSamples &&
			a.head.opts.SamplesPerChunk > int(maxInlineSamples) {
			if s.inlineN == 0 {
				// 首个 inline 样本，记录 openMinT（后续创建 chunk 时使用）。
				s.openMinT = t
			}
			s.inlineTs[s.inlineN] = t
			s.inlineVal[s.inlineN] = v
			s.inlineN++
			if t > s.lastTs {
				s.lastTs = t
			}
			// 更新 openMaxT 以便乱序检测和 blockReader 使用。
			if t > s.openMaxT {
				s.openMaxT = t
			}
			if appended == 0 {
				firstOK = t
			}
			lastOK = t
			appended++
			continue
		}

		// inline 已满或已有 open chunk：确保有 chunk，并将 inline 回填。
		if s.openChunk == nil {
			// 需要创建 chunk。如果有 inline 样本则从 openMinT 开始；
			// 否则从当前样本 t 开始。
			chunkStartT := t
			if s.inlineN > 0 {
				chunkStartT = s.openMinT
			}
			if a.ensureOpenChunk(s, chunkStartT, chunkenc.EncXOR) {
				a.head.metrics.chunksCreated.Inc()
			}
			if s.inlineN > 0 {
				s.flushInlineToChunk()
			}
		}

		if err := a.maybeCutChunk(s, t, chunkenc.EncXOR); err != nil {
			if appended > 0 {
				a.head.metrics.samplesAppended.Add(appended)
				a.head.updateMinMaxTime(firstOK)
				if lastOK != firstOK {
					a.head.updateMinMaxTime(lastOK)
				}
			}
			return err
		}

		s.openApp.Append(t, v)
		if t > s.openMaxT {
			s.openMaxT = t
		}
		if t > s.lastTs {
			s.lastTs = t
		}

		if appended == 0 {
			firstOK = t
		}
		lastOK = t
		appended++
	}

	if appended > 0 {
		a.head.metrics.samplesAppended.Add(appended)
		a.head.updateMinMaxTime(firstOK)
		if lastOK != firstOK {
			a.head.updateMinMaxTime(lastOK)
		}
	}
	return nil
}

// Rollback 丢弃样本，但保留已分配的新 series（须写入 WAL）。
//
// 仅当存在 pendingSeries 需要落 WAL 时才获取 appenderMtx.RLock；
// 纯粹的 "append 失败/放弃" 路径无需阻塞 SelfCompact。
func (a *appender) Rollback() error {
	defer a.reset()
	if len(a.pendingSeries) == 0 {
		return nil
	}
	a.head.appenderMtx.RLock()
	defer a.head.appenderMtx.RUnlock()
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
//
// litehead 只支持 float samples，调用方始终传 EncXOR；因此直接创建 XOR chunk，
// 不再做多级 encoding fallback。
func (a *appender) cutNewChunkLocked(s *memSeries, t int64, _ chunkenc.Encoding) bool {
	chk := chunkenc.NewXORChunk()
	app, _ := chk.Appender()
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
//
// 性能要点：series 与 samples 两条 record 通过**一次** wal.Log(a, b) 提交，
// 利用 WL.Log 的 variadic 语义：内部只取一次 mtx、只在最后一条 record 做
// page flush。等价于老实现"分两次 Log 各 fsync"的对半开销；实测 Commit
// 热路径可见降低。
//
// 实现细节：两条 record 需要各自独立的 []byte，不能复用同一缓冲区，否则
// 第二次 encode 会覆盖第一次的内容。从 bufPool 拿两块 buf：pool Get/Put
// 无锁且便宜，足以抵消一次重新分配的风险。
func (a *appender) logWAL() error {
	hasSeries := len(a.pendingSeries) > 0
	hasSamples := len(a.pendingSamples) > 0
	if !hasSeries && !hasSamples {
		return nil
	}

	var enc record.Encoder

	var seriesBufPtr, samplesBufPtr *[]byte
	var seriesBuf, samplesBuf []byte
	defer func() {
		if seriesBufPtr != nil {
			*seriesBufPtr = seriesBuf[:0]
			a.head.bufPool.Put(seriesBufPtr)
		}
		if samplesBufPtr != nil {
			*samplesBufPtr = samplesBuf[:0]
			a.head.bufPool.Put(samplesBufPtr)
		}
	}()

	var recs [][]byte
	if hasSeries {
		seriesBufPtr = a.head.bufPool.Get().(*[]byte)
		seriesBuf = enc.Series(a.pendingSeries, (*seriesBufPtr)[:0])
		recs = append(recs, seriesBuf)
	}
	if hasSamples {
		samplesBufPtr = a.head.bufPool.Get().(*[]byte)
		samplesBuf = enc.Samples(a.pendingSamples, (*samplesBufPtr)[:0])
		recs = append(recs, samplesBuf)
	}

	if err := a.head.wal.Log(recs...); err != nil {
		return errors.Wrap(err, "log WAL")
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
//
// 注意：本方法**不**释放 appenderMtx.RLock。自 B1 修复起，RLock 不再由
// Appender() 获取，而是由 Commit/Rollback 在真正落盘/写 WAL 的区间内
// 细粒度持有。
func (a *appender) reset() {
	head := a.head
	a.pendingSeries = a.pendingSeries[:0]
	a.pendingSamples = a.pendingSamples[:0]
	a.sampleSeries = a.sampleSeries[:0]
	// 从全局原子计数器获取新的唯一 batchGen：下次 Append 时，memSeries 上存储的
	// 旧 batchGen 不再匹配，等价于清空旧的批内乱序检测状态。O(1) 成本且保证
	// 跨 appender 实例不冲突。
	a.batchGen = head.nextBatchGen.Inc()
	head.appenderPool.Put(a)
}
