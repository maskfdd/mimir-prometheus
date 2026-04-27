package litehead

import (
	"context"
	"math"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// run 是后台 goroutine，周期性检查是否需要 compactHead 或 truncateWAL。
func (h *Head) run() {
	defer close(h.donec)

	ticker := time.NewTicker(h.opts.FlushCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopc:
			return
		case <-ticker.C:
			if h.shouldFlush() {
				if err := h.compactHead(); err != nil {
					level.Warn(h.logger).Log("msg", "periodic compactHead failed", "err", err)
				}
			}
		}
	}
}

// shouldFlush 判断是否达到 flush 阈值。
// 与标准 Head 的 compactable() 一致： MaxT - MinT > 1.5 * ChunkRange 时，
// 取左侧一个 ChunkRange 窗口 compactHead。
func (h *Head) shouldFlush() bool {
	mint := h.MinTime()
	maxt := h.MaxTime()
	if mint == math.MinInt64 || maxt == math.MinInt64 {
		return false
	}
	return maxt-mint > (h.opts.ChunkRange*3)/2
}

// compactHead 切出左侧一个 ChunkRange 宽度的窗口，compact 成 block。
// 语义对齐 tsdb.DB.compactHead。
func (h *Head) compactHead() error {
	h.flushMtx.Lock()
	defer h.flushMtx.Unlock()

	mint := h.MinTime()
	if mint == math.MinInt64 {
		return nil
	}
	maxt := rangeForTimestamp(mint, h.opts.BlockDuration) - 1
	return h.compactHeadWindow(mint, maxt)
}

// flushBlocking 被同步路径调用（比如 mmappedChunks[] 满了、Close 时）。
// 它会串行地把当前所有数据 flush 出去，绝不允许丢数据。
//
// 注意：调用方不应该已经持有 h.flushMtx，否则会死锁。
// 标准 Head 没有直接对应的方法（它的 Close 不会把整个 Head compact 完），
// 所以保留 flushBlocking 命名。
func (h *Head) flushBlocking() error {
	h.flushMtx.Lock()
	defer h.flushMtx.Unlock()

	mint := h.MinTime()
	maxt := h.MaxTime()
	if mint == math.MinInt64 || maxt == math.MinInt64 {
		return nil
	}
	for mint <= maxt {
		winMaxt := rangeForTimestamp(mint, h.opts.BlockDuration) - 1
		if winMaxt > maxt {
			winMaxt = maxt
		}
		// gcSeries=false：flushBlocking 可能被 appender 在持有 *memSeries
		// 指针的情况下调用（sealed 数组溢出的保底路径），这里不能 sweep
		// 任何 series，否则会把 appender 手里的 *memSeries 清零 + 还给
		// pool，引发并发 UAF。series 的真正回收留给周期 flush。
		if err := h.compactHeadWindowOpts(mint, winMaxt, false); err != nil {
			return err
		}
		mint = winMaxt + 1
	}
	return nil
}

// tryFlushAll 在 Close 时调用，把当前内存中剩余的数据一次性 flush 出去。
// 与 flushBlocking 的区别：tryFlushAll 在最后一轮 compact 时会做 GC
// （gcSeries=true），因为 Close 路径下不会有 appender 持有 *memSeries 指针。
func (h *Head) tryFlushAll() error {
	h.flushMtx.Lock()
	defer h.flushMtx.Unlock()

	mint := h.MinTime()
	maxt := h.MaxTime()
	if mint == math.MinInt64 || maxt == math.MinInt64 {
		return nil
	}
	for mint <= maxt {
		winMaxt := rangeForTimestamp(mint, h.opts.BlockDuration) - 1
		if winMaxt > maxt {
			winMaxt = maxt
		}
		// 最后一个窗口做 GC，前面的窗口不做（与 flushBlocking 一致）。
		isLast := winMaxt >= maxt
		if err := h.compactHeadWindowOpts(mint, winMaxt, isLast); err != nil {
			return err
		}
		mint = winMaxt + 1
	}
	return nil
}

// compactHeadWindow 把 [mint, maxt] 范围内的数据整理成一个 block。
// 语义对齐 tsdb.DB.compactHead 中的单轮 Write。
//
// 实现策略（方案 B：直接把 litehead 当作 BlockReader 喂给 compactor）：
//  1. 构造 blockReader，在它内部对 refTab 做一份"series 快照"
//     （仅记录 labels、mmappedChunk 引用、open chunk 标记），避免窗口内
//     的并发 append / spill 打乱 Index/Series 的返回结果；
//  2. 调用 LeveledCompactor.Write()，它会通过 BlockReader 的 Index/Chunks
//     直接从 ChunkDiskMapper + open chunk 取字节写 block；
//  3. 成功后调用 truncateMemory + truncateWAL，语义与标准 Head 对齐。
//
// 相比方案 A（临时 Head + WAL 回放），本路径省掉了样本的 "解码 → 重新
// append → 再编码" 回路，flush 期间几乎不会在堆上重复整批样本。
// 已知限制：labelCatalog arena 追加的 labels 仍然常驻，直到触发 rebuild。
func (h *Head) compactHeadWindow(mint, maxt int64) error {
	return h.compactHeadWindowOpts(mint, maxt, true)
}

// compactHeadWindowOpts 是 compactHeadWindow 的内部实现。gcSeries=true 时会
// 调用 truncateMemory（含 sweepDeadSeries），适合周期性 flush 和 Close；
// gcSeries=false 时只做 mmappedChunks / CDM 清理，不动 refTab/hashIdx，
// 适合 appender 触发的 forced flush（那时 appender 还握着 *memSeries 指针）。
func (h *Head) compactHeadWindowOpts(mint, maxt int64, gcSeries bool) error {
	h.metrics.compactionsTriggered.Inc()
	start := time.Now()

	prevMinValidTime := h.appendableMinValidTime()
	h.setMinValidTime(maxt + 1)

	br := newBlockReader(h, mint, maxt)
	if len(br.series) == 0 {
		level.Info(h.logger).Log("msg", "compact head window empty, skipping", "mint", mint, "maxt", maxt)
		if gcSeries {
			h.truncateMemory(maxt)
		} else {
			h.truncateMemoryKeepSeries(maxt)
		}
		if err := h.truncateWAL(maxt); err != nil {
			level.Warn(h.logger).Log("msg", "post-compact truncate WAL failed", "err", err)
		}
		h.metrics.compactionDuration.Observe(time.Since(start).Seconds())
		return nil
	}

	ctx := context.Background()
	compactor, err := tsdb.NewLeveledCompactor(ctx, nil, h.logger,
		[]int64{h.opts.BlockDuration}, chunkenc.NewPool(), nil, true)
	if err != nil {
		h.metrics.compactionsFailed.Inc()
		if prevMinValidTime > math.MinInt64 {
			h.minValidTime.Store(prevMinValidTime)
		}
		return errors.Wrap(err, "create leveled compactor")
	}
	// block 区间是半开的 [mint, maxt+1)，与原生 Head flush 约定一致。
	if _, err := compactor.Write(h.dir, br, mint, maxt+1, nil); err != nil {
		h.metrics.compactionsFailed.Inc()
		if prevMinValidTime > math.MinInt64 {
			h.minValidTime.Store(prevMinValidTime)
		}
		return errors.Wrap(err, "compactor write from litehead block reader")
	}

	// compact 成功：先 truncate 内存，再 truncateWAL。顺序要紧：先把窗口外
	// 的 mmappedChunks / series 清理掉，truncateWAL 的 checkpoint keep 函数
	// 就可以直接用 "series 还在 refTab" 作为条件。
	if gcSeries {
		h.truncateMemory(maxt)
	} else {
		h.truncateMemoryKeepSeries(maxt)
	}

	if err := h.truncateWAL(maxt); err != nil {
		level.Warn(h.logger).Log("msg", "post-compact truncate WAL failed", "err", err)
	}

	h.metrics.compactionDuration.Observe(time.Since(start).Seconds())
	level.Info(h.logger).Log("msg", "write head compacted", "mint", mint, "maxt", maxt, "duration", time.Since(start))
	return nil
}

// truncateMemory 是 compactHeadWindow 成功后的内存清理步骤，对齐标准 Head 的
// truncateMemory 语义。语义对齐标准 Head 的 truncateMemory。
//
// 两阶段清理：
//  1. truncateMmapped：压缩每条 series 的 mmappedChunks、尝试释放已落盘的
//     open chunk；safe for concurrent appenders。
//  2. sweepDeadSeries：回收那些已经无任何 chunk / lastTs 落后的 series
//     的 refTab / hashIdx 槽位；仅在"caller 不会再访问同一 series 指针"
//     的路径上调用（即非 appender 强制 flush 路径）。
func (h *Head) truncateMemory(maxt int64) {
	h.advanceMinTime(maxt)
	h.truncateMmapped(maxt)
	h.sweepDeadSeries(maxt)
}

// truncateMemoryKeepSeries 用于 appender 触发的 forced flush：只清 mmappedChunks
// 和 CDM，不删 series，因为此时 caller 还握着 *memSeries 指针，删掉会 UAF。
// 等下次周期 flush 或 Close 时再做 sweepDeadSeries。
func (h *Head) truncateMemoryKeepSeries(maxt int64) {
	h.advanceMinTime(maxt)
	h.truncateMmapped(maxt)
}

// advanceMinTime 把 minTime 向前推进到 maxt+1；如果没有更多样本，则保持哨兵值。
func (h *Head) advanceMinTime(maxt int64) {
	for {
		cur := h.minTime.Load()
		if cur > maxt || cur == math.MaxInt64 {
			return
		}
		if h.minTime.CompareAndSwap(cur, maxt+1) {
			return
		}
	}
}

// truncateMmapped 遍历所有 series，丢弃落在 flushMaxt 之前的 mmappedChunks
// 条目，清空 openMaxT <= flushMaxt 的 open chunk，并把可释放的
// ChunkDiskMapper 文件段释放掉。不触碰 series 在 refTab/hashIdx 的槽位。
//
// 不会出现 s 指针悬挂：即便 caller（比如 appender）还握着同一 series 指针，
// 这里对 s 的改动都是字段级别的清理，s 本身仍活在 refTab 里。
func (h *Head) truncateMmapped(flushMaxt int64) {
	var (
		minAliveFileNo uint32 = math.MaxUint32
		aliveSealed    int
	)

	h.refTab.forEach(func(s *memSeries) {
		s.mu.Lock()
		// 压缩 mmappedChunks：保留 maxTime > flushMaxt 的条目。
		n := 0
		for i := 0; i < int(s.mmappedChunksCount); i++ {
			if s.mmappedChunks[i].maxTime > flushMaxt {
				s.mmappedChunks[n] = s.mmappedChunks[i]
				n++
				if fileNo, _ := s.mmappedChunks[i].ref.Unpack(); uint32(fileNo) < minAliveFileNo {
					minAliveFileNo = uint32(fileNo)
				}
			}
		}
		for i := n; i < int(s.mmappedChunksCount); i++ {
			s.mmappedChunks[i] = mmappedChunk{}
		}
		s.mmappedChunksCount = uint8(n)
		aliveSealed += n

		// open chunk：如果所有样本都已经进了本次 flush 的 block，释放掉。
		// 跨越 flushMaxt 的 open chunk 保留——它里面还有未 flush 的样本。
		if s.openChunk != nil && s.openChunk.NumSamples() > 0 &&
			s.openMaxT != math.MinInt64 && s.openMaxT <= flushMaxt {
			s.openChunk = nil
			s.openApp = nil
			s.openMinT = 0
			s.openMaxT = math.MinInt64
			s.nextAt = 0
		}
		s.mu.Unlock()
	})

	// CDM Truncate：只有确定还有活跃 sealed chunk 引用的文件号才 truncate，
	// 否则兑底不删（保守，避免把不该删的 chunk 文件扫掉）。
	if aliveSealed > 0 && minAliveFileNo != math.MaxUint32 {
		if err := h.chunkDiskMapper.Truncate(minAliveFileNo); err != nil {
			level.Warn(h.logger).Log("msg", "chunk disk mapper truncate", "err", err, "minFile", minAliveFileNo)
		}
	}

	h.metrics.labelCatalogSize.Set(float64(h.labelCat.size()))
	h.metrics.labelCatalogCount.Set(float64(h.labelCat.count()))
	h.metrics.labelCatalogSymbolsSize.Set(float64(h.labelCat.symbolsSize()))
	h.metrics.labelCatalogSymbolsCount.Set(float64(h.labelCat.symbolsCount()))
}

// sweepDeadSeries 回收那些"不再有任何 chunk、并且最新样本时间 < flushMaxt"
// 的 series 的 refTab / hashIdx 槽位。只在确信 caller 不会再访问同一 series
// 指针的路径里调用（比如周期性 flush、Close），避免把 appender 正持有的
// *memSeries 打到 pool 后再被继续写坏。
func (h *Head) sweepDeadSeries(flushMaxt int64) {
	var (
		deadRefs      []chunks.HeadSeriesRef
		deadLabelsIDs []uint32
	)

	h.refTab.forEach(func(s *memSeries) {
		s.mu.Lock()
		dead := s.openChunk == nil && s.mmappedChunksCount == 0 && s.lastTs <= flushMaxt
		if dead {
			deadRefs = append(deadRefs, s.ref)
			deadLabelsIDs = append(deadLabelsIDs, s.labelsID)
		}
		s.mu.Unlock()
	})

	// labelsID 对应的 labels 仍然驻留在 labelCatalog arena 里 —— append-only
	// 的已知限制。真正的回收只能在 arena 重建时做；本期让它留着，下游 Hash
	// 查不到引用时就是垃圾。
	for i, ref := range deadRefs {
		s := h.refTab.get(ref)
		if s == nil {
			continue
		}
		lset := h.labelCat.get(deadLabelsIDs[i])
		h.hashIdx.delete(lset.Hash(), ref)
		h.refTab.del(ref)
		*s = memSeries{}
		h.seriesPool.Put(s)
		h.metrics.seriesActive.Dec()
		h.metrics.seriesRemoved.Inc()
	}
}

// advanceMinTAfterFlush 已被 truncateMemory / advanceMinTime 替代，不再存在。

// truncateWAL 创建 WAL checkpoint，并 truncate 之前的段。语义对齐 tsdb.Head.truncateWAL。
//
// keep 函数：当对应 ref 仍在 refTable 里（即未被 GC）时保留。
// 因为 truncateMemory 已经先跑过一遍，这里留下的 series 就是 "窗口之后还
// 可能被写入的 series"，必须在 checkpoint 里保留它们的 Series 记录。
func (h *Head) truncateWAL(mint int64) error {
	start := time.Now()

	first, last, err := wlog.Segments(h.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "find WAL segments")
	}
	if _, err := h.wal.NextSegment(); err != nil {
		return errors.Wrap(err, "roll to next WAL segment")
	}
	last--
	if last <= first {
		return nil
	}
	last = first + (last-first)*2/3
	if last <= first {
		return nil
	}

	keep := func(id chunks.HeadSeriesRef) bool {
		return h.refTab.get(id) != nil
	}

	if _, err := wlog.Checkpoint(h.logger, h.wal, first, last, keep, mint); err != nil {
		h.metrics.checkpointCreationFail.Inc()
		return errors.Wrap(err, "create checkpoint")
	}
	if err := h.wal.Truncate(last + 1); err != nil {
		// truncate 失败不是致命错误；旧段会在下次尝试时被覆盖。
		level.Warn(h.logger).Log("msg", "truncate WAL", "err", err)
	}
	if err := wlog.DeleteCheckpoints(h.wal.Dir(), last); err != nil {
		level.Warn(h.logger).Log("msg", "delete old checkpoints", "err", err)
	}

	h.metrics.checkpointCreationTotal.Inc()
	h.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())
	return nil
}
