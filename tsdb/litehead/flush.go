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

// compactable 判断是否达到 flush 阈值：MaxT - MinT > chunkRange/2*3。
func (h *Head) compactable() bool {
	mint := h.MinTime()
	maxt := h.MaxTime()
	if mint == math.MinInt64 || maxt == math.MinInt64 {
		return false
	}
	return maxt-mint > h.chunkRange.Load()/2*3
}

// flushBlocking 同步把所有数据 flush 出去。
// gcSeries=false：避免回收 appender 正持有的 *memSeries。
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
		// gcSeries=false：不 sweep series，避免并发 UAF。
		if err := h.compactHeadWindowOpts(mint, winMaxt, false); err != nil {
			return err
		}
		mint = winMaxt + 1
	}
	return nil
}

// tryFlushAll 把内存中所有数据 flush 出去，最后一轮做 GC。
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

// compactHeadWindowOpts 把 [mint, maxt] 范围内的数据写成一个 block。
// gcSeries=true 时会 sweepDeadSeries，否则只清理 mmappedChunks/CDM。
func (h *Head) compactHeadWindowOpts(mint, maxt int64, gcSeries bool) error {
	h.metrics.compactionsTriggered.Inc()
	start := time.Now()

	prevMinValidTime := h.appendableMinValidTime()
	h.setMinValidTime(maxt + 1)

	br := newBlockReader(h, mint, maxt)
	// done() 释放 newBlockReader 保留的引用；无论是 empty-window 快速路径还是
	// compactor 返回后的正常路径，都必须走到这里，才能把 pool 借出的
	// open-chunk scratch buffer 归还回去。
	defer br.done()
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
	// block 区间 [mint, maxt+1)。
	if _, err := compactor.Write(h.dir, br, mint, maxt+1, nil); err != nil {
		h.metrics.compactionsFailed.Inc()
		if prevMinValidTime > math.MinInt64 {
			h.minValidTime.Store(prevMinValidTime)
		}
		return errors.Wrap(err, "compactor write from litehead block reader")
	}

	// 先 truncate 内存，再 truncateWAL。
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

// truncateMemory 清理已落盘的内存数据并回收无用 series。
func (h *Head) truncateMemory(maxt int64) {
	h.advanceMinTime(maxt)
	h.truncateMmapped(maxt)
	h.sweepDeadSeries(maxt)
}

// truncateMemoryKeepSeries 只清 mmappedChunks/CDM，不删 series。
func (h *Head) truncateMemoryKeepSeries(maxt int64) {
	h.advanceMinTime(maxt)
	h.truncateMmapped(maxt)
}

// advanceMinTime 把 minTime 推进到 maxt+1。
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

// truncateMmapped 丢弃 flushMaxt 之前的 mmappedChunks 和已落盘的 open chunk，
// 并释放对应的 ChunkDiskMapper 文件段。
func (h *Head) truncateMmapped(flushMaxt int64) {
	var (
		minAliveFileNo uint32 = math.MaxUint32
		aliveSealed    int
	)

	h.refTab.forEach(func(s *memSeries) {
		s.mu.Lock()
		// 压缩 sealed chunks：保留 maxTime > flushMaxt 的条目，顺带上报
		// 被保留条目覆盖的最小文件号，用于后续 CDM Truncate。
		s.retainSealedAfter(flushMaxt, func(mc mmappedChunk) {
			if fileNo, _ := mc.ref.Unpack(); uint32(fileNo) < minAliveFileNo {
				minAliveFileNo = uint32(fileNo)
			}
		})
		aliveSealed += s.sealedLen()

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

// sweepDeadSeries 回收无 chunk 且 lastTs <= flushMaxt 的 series。
func (h *Head) sweepDeadSeries(flushMaxt int64) {
	var (
		deadRefs      []chunks.HeadSeriesRef
		deadLabelsIDs []uint32
	)

	h.refTab.forEach(func(s *memSeries) {
		s.mu.Lock()
		dead := s.openChunk == nil && s.sealedLen() == 0 && s.lastTs <= flushMaxt
		if dead {
			deadRefs = append(deadRefs, s.ref)
			deadLabelsIDs = append(deadLabelsIDs, s.labelsID)
		}
		s.mu.Unlock()
	})

	// labelsID 驻留在 labelCatalog arena 里（append-only），仅在 arena 重建时回收。
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
		h.numSeries.Dec()
		h.metrics.seriesActive.Dec()
		h.metrics.seriesRemoved.Inc()
	}
}

// truncateWAL 创建 checkpoint 并 truncate 旧 WAL 段。
func (h *Head) truncateWAL(mint int64) error {
	if mint <= h.lastWALTruncationTime.Load() {
		return nil
	}
	start := time.Now()
	h.lastWALTruncationTime.Store(mint)

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
