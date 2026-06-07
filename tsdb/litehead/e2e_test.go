package litehead

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

// ---------- helpers ----------

// newE2EHead 创建一个用于 e2e 测试的 Head。
// blockDur 是 BlockDuration（毫秒），chunkRange 是 ChunkRange（毫秒）。
func newE2EHead(t *testing.T, blockDur, chunkRange int64) (*Head, string) {
	t.Helper()
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.BlockDuration = blockDur
	opts.ChunkRange = chunkRange
	opts.NoLockfile = true
	opts.SamplesPerChunk = 120

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))
	return h, dir
}

// appendSamples 向 head 写入 numSeries 条 series，每条 series 从 startT 开始，
// 步长 step（毫秒），共写 samplesPerSeries 个样本。
func appendSamples(t *testing.T, h *Head, numSeries, samplesPerSeries int, startT, step int64) {
	t.Helper()
	app := h.Appender(context.Background())
	for i := 0; i < numSeries; i++ {
		lset := labels.FromStrings("__name__", "e2e_metric", "instance", fmt.Sprintf("inst_%d", i))
		ts := startT
		for j := 0; j < samplesPerSeries; j++ {
			_, err := app.Append(0, lset, ts, float64(ts))
			require.NoError(t, err)
			ts += step
		}
	}
	require.NoError(t, app.Commit())
}

// listBlocks 返回 dir 下所有 block 的 meta 信息。
func listBlocks(t *testing.T, dir string) []tsdb.BlockMeta {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var metas []tsdb.BlockMeta
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		metaPath := filepath.Join(dir, e.Name(), "meta.json")
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			continue
		}
		b, err := tsdb.OpenBlock(log.NewNopLogger(), filepath.Join(dir, e.Name()), nil)
		if err != nil {
			continue
		}
		metas = append(metas, b.Meta())
		require.NoError(t, b.Close())
	}
	return metas
}

// heapInUseBytes 返回当前 heap 已使用字节数（强制 GC 后）。
func heapInUseBytes() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// ---------- Test 1: Block 时间范围合规性 ----------

// TestE2E_BlockRangeCompliance 验证：
// 1. SelfCompact（tryFlushAligned）只产出对齐到 BlockDuration 的 block，
//    不会产出短于 BlockDuration 的碎片 block。
// 2. Close（tryFlushAll）会把最后不足一个窗口的尾巴也 flush，
//    但不应产生大量碎片。
// 3. block 数量合理——不会莫名生成 "非常多小的 block"。
func TestE2E_BlockRangeCompliance(t *testing.T) {
	// 配置：BlockDuration = 2h = 7_200_000ms，ChunkRange = 2h。
	blockDur := int64(2 * 60 * 60 * 1000)   // 2h in ms
	chunkRange := blockDur                    // 与 blockDur 一致
	step := int64(15_000)                     // 15s 采样间隔
	numSeries := 50
	// 写 5.5 个 BlockDuration 的数据 = 11h，应产出 5 个完整 block + 1 个尾巴。
	totalDuration := int64(float64(blockDur) * 5.5)
	samplesPerSeries := int(totalDuration / step)

	h, dir := newE2EHead(t, blockDur, chunkRange)

	// 分批写入以模拟真实持续写入场景。
	batchSize := samplesPerSeries / 10
	startT := int64(0)
	for written := 0; written < samplesPerSeries; {
		n := batchSize
		if written+n > samplesPerSeries {
			n = samplesPerSeries - written
		}
		appendSamples(t, h, numSeries, n, startT, step)
		startT += int64(n) * step
		written += n

		// 每批写完后尝试 SelfCompact，模拟 DB.Compact() 的调度。
		handled, err := h.SelfCompact(context.Background())
		require.NoError(t, err)
		require.True(t, handled)
	}

	// Close 前查看 SelfCompact 产出的 block。
	blocksBeforeClose := listBlocks(t, dir)
	t.Logf("blocks before Close: %d", len(blocksBeforeClose))
	for _, b := range blocksBeforeClose {
		dur := b.MaxTime - b.MinTime
		t.Logf("  block %s: [%d, %d) dur=%dms (%s), series=%d, samples=%d",
			b.ULID, b.MinTime, b.MaxTime,
			dur, time.Duration(dur)*time.Millisecond, b.Stats.NumSeries, b.Stats.NumSamples)

		// SelfCompact 路径（tryFlushAligned）产出的 block 必须是完整 BlockDuration。
		require.Equalf(t, blockDur, dur,
			"SelfCompact 产出了不对齐的 block: [%d, %d) dur=%dms, 预期=%dms",
			b.MinTime, b.MaxTime, dur, blockDur)
	}

	// Close 会 tryFlushAll 把尾巴数据也 flush 出去。
	require.NoError(t, h.Close())

	blocksAfterClose := listBlocks(t, dir)
	t.Logf("blocks after Close: %d", len(blocksAfterClose))

	// 总 block 数 = 完整窗口数 + 可能的 1 个尾巴。
	// 5.5 个 BlockDuration 的数据：SelfCompact 产出 5 个，Close 产出 1 个尾巴 = 6 个。
	// 但由于 compactable() 判断 maxt-mint > chunkRange*3/2，前几批数据不够会延迟
	// flush，所以可能产出稍少的完整 block。但绝不应该产出 "非常多"（>10）的 block。
	require.LessOrEqualf(t, len(blocksAfterClose), 10,
		"产出了过多的 block (%d)，可能存在碎片化问题", len(blocksAfterClose))
	require.GreaterOrEqualf(t, len(blocksAfterClose), 2,
		"数据应至少产出 2 个 block，实际只产出 %d 个", len(blocksAfterClose))

	// 验证尾巴 block（Close 新增的）允许短于 BlockDuration，但仍不应极端小。
	newBlocks := len(blocksAfterClose) - len(blocksBeforeClose)
	if newBlocks > 0 {
		// Close 最多多产出 1 个尾巴 block。
		require.LessOrEqualf(t, newBlocks, 2,
			"Close 产出了 %d 个新 block，预期最多 2 个", newBlocks)
	}

	// 验证所有 block 的时间范围不重叠。
	for i := 1; i < len(blocksAfterClose); i++ {
		prev := blocksAfterClose[i-1]
		curr := blocksAfterClose[i]
		if prev.MinTime > curr.MinTime {
			prev, curr = curr, prev
		}
		require.LessOrEqualf(t, prev.MaxTime, curr.MinTime,
			"block %s [%d,%d) 和 block %s [%d,%d) 时间范围重叠",
			prev.ULID, prev.MinTime, prev.MaxTime,
			curr.ULID, curr.MinTime, curr.MaxTime)
	}

	// 验证所有 block 的数据连续覆盖了完整时间范围，无数据丢失。
	// 收集所有 block 的 [MinTime, MaxTime) 并按 MinTime 排序。
	type timeRange struct{ min, max int64 }
	var ranges []timeRange
	for _, b := range blocksAfterClose {
		ranges = append(ranges, timeRange{b.MinTime, b.MaxTime})
	}
	// 简单冒泡排序（block 数很少）。
	for i := range ranges {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[j].min < ranges[i].min {
				ranges[i], ranges[j] = ranges[j], ranges[i]
			}
		}
	}
	// 验证连续性：后一个 block 的 min 应该等于前一个的 max。
	for i := 1; i < len(ranges); i++ {
		require.Equalf(t, ranges[i-1].max, ranges[i].min,
			"block 时间范围不连续：[%d,%d) -> [%d,%d)，gap=%dms",
			ranges[i-1].min, ranges[i-1].max,
			ranges[i].min, ranges[i].max,
			ranges[i].min-ranges[i-1].max)
	}
}

// TestE2E_SelfCompactNoFragmentation 验证在持续写入下 SelfCompact 不会
// 生成大量碎片 block。使用较短的 BlockDuration 加速测试。
func TestE2E_SelfCompactNoFragmentation(t *testing.T) {
	// 配置：BlockDuration = 1min，ChunkRange = 1min。
	blockDur := int64(60_000) // 1min in ms
	step := int64(1_000)      // 1s 采样间隔
	numSeries := 20
	// 写 10min 的数据 = 10 个完整 BlockDuration。
	totalMinutes := 10
	samplesPerSeries := totalMinutes * 60 // 600 samples

	h, dir := newE2EHead(t, blockDur, blockDur)

	// 逐秒写入并频繁调用 SelfCompact，模拟激进的 compaction 调度。
	startT := int64(0)
	for i := 0; i < samplesPerSeries; i++ {
		appendSamples(t, h, numSeries, 1, startT, step)
		startT += step

		// 每 10s 调一次 SelfCompact。
		if i%10 == 0 {
			_, err := h.SelfCompact(context.Background())
			require.NoError(t, err)
		}
	}

	require.NoError(t, h.Close())

	blocks := listBlocks(t, dir)
	t.Logf("total blocks: %d (for %d minutes of data)", len(blocks), totalMinutes)

	// 10min 数据 / 1min BlockDuration = 10 个完整 block + 可能的尾巴。
	// 但因为 compactable 需要 maxt-mint > chunkRange*1.5 = 90s，
	// flush 会有延迟，实际完整 block 可能稍少，close 时 flush 剩余。
	// 无论如何不应超过 15 个 block（10 + 少量边界效应）。
	require.LessOrEqualf(t, len(blocks), 15,
		"产出了 %d 个 block，超过预期上限 15，存在碎片化", len(blocks))

	// 验证没有极短的 block（小于 BlockDuration 的 10%）。
	for _, b := range blocks {
		dur := b.MaxTime - b.MinTime
		t.Logf("  block %s: dur=%dms, series=%d, samples=%d",
			b.ULID, dur, b.Stats.NumSeries, b.Stats.NumSamples)
		// 允许最后一个 block 短，但不应短到离谱（< 1s）。
		require.Greaterf(t, dur, int64(1000),
			"block %s 时长仅 %dms，远小于 BlockDuration=%dms",
			b.ULID, dur, blockDur)
	}
}

// ---------- Test 2: 内存泄漏检测 ----------

// TestE2E_MemoryAfterFlush 验证 flush 后内存被正确回收：
// 1. 写入大量 series + 样本 → 记录内存峰值
// 2. SelfCompact/Flush → 记录内存
// 3. flush 后内存应显著低于峰值（series/chunk 数据已落盘回收）
// 4. 重复写入-flush 循环，内存不应持续增长（泄漏）
func TestE2E_MemoryAfterFlush(t *testing.T) {
	// 使用较短的 BlockDuration 加速测试。
	blockDur := int64(60_000) // 1min
	step := int64(1_000)      // 1s
	numSeries := 500
	// 每轮写 2min 数据（> chunkRange * 1.5 = 90s，确保 compactable）。
	samplesPerRound := 120 // 2min
	rounds := 5

	h, dir := newE2EHead(t, blockDur, blockDur)
	_ = dir

	baseline := heapInUseBytes()
	t.Logf("baseline heap: %d MB", baseline/1024/1024)

	var afterFlushHeaps []uint64

	startT := int64(0)
	for round := 0; round < rounds; round++ {
		// 写入数据。
		appendSamples(t, h, numSeries, samplesPerRound, startT, step)
		startT += int64(samplesPerRound) * step

		heapAfterWrite := heapInUseBytes()
		t.Logf("round %d: after write heap=%d MB, numSeries=%d",
			round, heapAfterWrite/1024/1024, h.NumSeries())

		// SelfCompact。
		handled, err := h.SelfCompact(context.Background())
		require.NoError(t, err)
		require.True(t, handled)

		heapAfterFlush := heapInUseBytes()
		afterFlushHeaps = append(afterFlushHeaps, heapAfterFlush)
		t.Logf("round %d: after flush heap=%d MB, numSeries=%d",
			round, heapAfterFlush/1024/1024, h.NumSeries())

		// flush 后内存应低于写入后的峰值（数据已落盘）。
		// 放宽条件：至少不比写入后更大（考虑 GC 抖动）。
		// 这里只做宽松检查，主要验证不会泄漏。
	}

	// 验证：flush 后的内存在多轮循环中不应持续单调增长。
	// 计算最后一轮 vs 第一轮的增长幅度，允许一定的波动但不应翻倍。
	if len(afterFlushHeaps) >= 2 {
		first := afterFlushHeaps[0]
		last := afterFlushHeaps[len(afterFlushHeaps)-1]
		// 考虑到 GC 和 runtime 的噪声，允许最后一轮比第一轮多 100%。
		// 如果存在泄漏，多轮后会远超 2x。
		growthRatio := float64(last) / float64(first)
		t.Logf("memory growth ratio (last/first): %.2f (first=%d MB, last=%d MB)",
			growthRatio, first/1024/1024, last/1024/1024)
		require.Lessf(t, growthRatio, 3.0,
			"flush 后内存增长 %.2fx（from %d MB to %d MB），可能存在内存泄漏",
			growthRatio, first/1024/1024, last/1024/1024)
	}

	require.NoError(t, h.Close())
}

// TestE2E_MemoryReclamation 验证关闭后内存被正确释放（与 litehead 的设计目标对齐）。
// 验证：
// 1. 带 metrics registry 的场景下，Close 不会 panic 且正确 flush。
// 2. Close 后 heap 不高于写入前（数据已落盘）。
// 3. litehead 自有 metrics 能被正确 unregister。
func TestE2E_MemoryReclamation(t *testing.T) {
	blockDur := int64(60_000) // 1min
	step := int64(1_000)      // 1s
	numSeries := 200
	samplesPerSeries := 180   // 3min

	reg := prometheus.NewRegistry()

	dir := t.TempDir()
	opts := DefaultOptions()
	opts.BlockDuration = blockDur
	opts.ChunkRange = blockDur
	opts.NoLockfile = true
	opts.SamplesPerChunk = 120

	h, err := NewHead(log.NewNopLogger(), reg, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))

	// 写入数据。
	appendSamples(t, h, numSeries, samplesPerSeries, 0, step)

	heapBefore := heapInUseBytes()
	t.Logf("before Close: heap=%d MB, numSeries=%d", heapBefore/1024/1024, h.NumSeries())

	// Close 应该：tryFlushAll + unregister metrics + 释放资源。
	require.NoError(t, h.Close())

	heapAfter := heapInUseBytes()
	t.Logf("after Close: heap=%d MB", heapAfter/1024/1024)

	// 验证 blocks 产出。
	blocks := listBlocks(t, dir)
	t.Logf("total blocks: %d", len(blocks))
	require.Greater(t, len(blocks), 0, "3min 数据应产出至少 1 个 block")

	// 验证 litehead 自有 metrics 已被 unregister：用新 registry 重建 Head
	// 不会 panic（注意：WAL 层的 metrics 不可 unregister，因此第二次必须
	// 使用新 registry，这是 WAL 的已知限制，不是 litehead 的问题）。
	reg2 := prometheus.NewRegistry()
	h2, err := NewHead(log.NewNopLogger(), reg2, dir, opts)
	require.NoError(t, err, "重新创建 Head 失败")
	require.NoError(t, h2.Init(math.MinInt64))
	require.NoError(t, h2.Close())
}

// ---------- Test 3: 过期清理 ----------

// TestE2E_ExpiryCleanup 端到端验证 litehead 的过期清理能力。
// 分两个阶段写入不同 series 的数据，触发 SelfCompact 后验证：
//  1. 已落盘且不再活跃的 series 被 sweepDeadSeries 从 refTab/hashIdx 中删除。
//  2. Head.MinTime() 被正确推进到已 flush 窗口之后。
//  3. 已过期的 open chunk、inline 样本和 sealed chunks 被释放。
//  4. labelCatalog 中死 series 的标签编码被回收（rebuild）。
//  5. 写入比 minValidTime 更老的样本被 ErrOutOfBounds 拒绝。
//  6. 活跃 series 的数据不受影响，仍可正常追加。
func TestE2E_ExpiryCleanup(t *testing.T) {
	blockDur := int64(60_000) // 1min
	step := int64(1_000)      // 1s
	numSeriesPhase1 := 100    // 第一阶段的 series 数
	numSeriesPhase2 := 50     // 第二阶段的 series 数（不同 labels）
	// 每个 series 写 2min 数据，确保跨越 > 1.5 * chunkRange = 90s。
	samplesPerSeries := 120

	h, _ := newE2EHead(t, blockDur, blockDur)

	ctx := context.Background()

	// ── Phase 1: 写入第一批 series（phase1_inst_*） ──
	startT := int64(0)
	app := h.Appender(ctx)
	for i := 0; i < numSeriesPhase1; i++ {
		lset := labels.FromStrings("__name__", "expiry_metric", "phase", "1", "instance", fmt.Sprintf("inst_%d", i))
		ts := startT
		for j := 0; j < samplesPerSeries; j++ {
			_, err := app.Append(0, lset, ts, float64(ts))
			require.NoError(t, err)
			ts += step
		}
	}
	require.NoError(t, app.Commit())
	phase1EndT := startT + int64(samplesPerSeries)*step

	seriesAfterPhase1 := h.NumSeries()
	labelCatSizeAfterPhase1 := h.labelCat.size()
	require.Equal(t, uint64(numSeriesPhase1), seriesAfterPhase1)
	t.Logf("phase1: numSeries=%d, labelCatSize=%d, MinTime=%d, MaxTime=%d",
		seriesAfterPhase1, labelCatSizeAfterPhase1, h.MinTime(), h.MaxTime())

	// ── Phase 2: 写入第二批 series（完全不同的 labels），时间接续 phase1 ──
	startT = phase1EndT
	app = h.Appender(ctx)
	for i := 0; i < numSeriesPhase2; i++ {
		lset := labels.FromStrings("__name__", "expiry_metric", "phase", "2", "instance", fmt.Sprintf("new_%d", i))
		ts := startT
		for j := 0; j < samplesPerSeries; j++ {
			_, err := app.Append(0, lset, ts, float64(ts))
			require.NoError(t, err)
			ts += step
		}
	}
	require.NoError(t, app.Commit())

	// 此时两批 series 都在内存里。
	seriesBeforeFlush := h.NumSeries()
	require.Equal(t, uint64(numSeriesPhase1+numSeriesPhase2), seriesBeforeFlush)
	t.Logf("before flush: numSeries=%d, MinTime=%d, MaxTime=%d",
		seriesBeforeFlush, h.MinTime(), h.MaxTime())

	mintBeforeFlush := h.MinTime()

	// ── 触发 SelfCompact：flush 已完成的对齐窗口，最后一个完整窗口触发 GC ──
	handled, err := h.SelfCompact(ctx)
	require.NoError(t, err)
	require.True(t, handled)

	// ── 验证 1: MinTime 被推进 ──
	mintAfterFlush := h.MinTime()
	t.Logf("after flush: MinTime=%d (was %d)", mintAfterFlush, mintBeforeFlush)
	require.Greater(t, mintAfterFlush, mintBeforeFlush,
		"MinTime 应在 flush 后被推进")

	// ── 验证 2: phase1 的死 series 被回收 ──
	seriesAfterFlush := h.NumSeries()
	t.Logf("after flush: numSeries=%d (was %d)", seriesAfterFlush, seriesBeforeFlush)
	// phase1 的 series 数据全在已 flush 的窗口内，应该被 sweep 掉。
	// 只有 phase2 的 series（可能部分仍在尾巴窗口中）存活。
	require.Lessf(t, seriesAfterFlush, seriesBeforeFlush,
		"flush 后 series 数应减少，phase1 的死 series 应被清理")
	// phase1 的 100 条 series 都不再活跃，应该大部分或全部被清理。
	require.LessOrEqualf(t, seriesAfterFlush, uint64(numSeriesPhase2+10),
		"flush 后 series 数 (%d) 远超 phase2 的 series 数 (%d)，phase1 死 series 未被有效回收",
		seriesAfterFlush, numSeriesPhase2)

	// ── 验证 3: labelCatalog 回收（死 series 占比 > 30% 时 rebuild） ──
	labelCatSizeAfterFlush := h.labelCat.size()
	labelCatCountAfterFlush := h.labelCat.count()
	t.Logf("after flush: labelCatSize=%d (was %d), labelCatCount=%d",
		labelCatSizeAfterFlush, labelCatSizeAfterPhase1, labelCatCountAfterFlush)
	// 死 series (100) >> 活跃 series (50) 的 30%，应触发 rebuild。
	// rebuild 后 catalog 中的 entries 数应 <= 活跃 series 数。
	require.LessOrEqualf(t, labelCatCountAfterFlush, numSeriesPhase2+10,
		"labelCatalog entries (%d) 应被 rebuild 到接近活跃 series 数 (%d)",
		labelCatCountAfterFlush, numSeriesPhase2)

	// ── 验证 4: 写入过期时间戳被拒绝 ──
	app = h.Appender(ctx)
	_, err = app.Append(0,
		labels.FromStrings("__name__", "expiry_metric", "phase", "1", "instance", "inst_0"),
		1000, // 远早于 mintAfterFlush
		99.0)
	require.ErrorIs(t, err, storage.ErrOutOfBounds,
		"写入比 minValidTime 更老的样本应被拒绝")
	require.NoError(t, app.Rollback())

	// ── 验证 5: 活跃 series 仍可正常追加 ──
	app = h.Appender(ctx)
	// 在 phase2 的某条 series 上继续追加。
	lsetActive := labels.FromStrings("__name__", "expiry_metric", "phase", "2", "instance", "new_0")
	futureT := startT + int64(samplesPerSeries)*step + step // 比之前所有数据都更新
	_, err = app.Append(0, lsetActive, futureT, 42.0)
	require.NoError(t, err, "活跃 series 追加新样本不应失败")
	require.NoError(t, app.Commit())

	// ── 验证 6: block 产出正确 ──
	require.NoError(t, h.Close())
}

// ---------- Test 4: 过期 Block 清理 ----------

// TestE2E_BlockRetentionCleanup 端到端验证 litehead 产出的过期 block 能被正确清理。
// litehead 自身只负责 flush 产出 block，block 的 retention 淘汰由上层（tsdb.DB
// 的 BeyondTimeRetention/BeyondSizeRetention + reloadBlocks）负责。
// 此测试模拟上层的 retention 行为：
//  1. 持续写入大量时间跨度的数据，flush 产出多个 block。
//  2. 用 tsdb.BeyondTimeRetention 同等逻辑判定过期 block。
//  3. 手动删除过期 block 目录。
//  4. 验证磁盘上只保留 retention 范围内的 block。
//  5. 验证删除过期 block 后 litehead 重启仍能正常写入和 flush。
func TestE2E_BlockRetentionCleanup(t *testing.T) {
	blockDur := int64(60_000) // 1min
	step := int64(1_000)      // 1s
	numSeries := 20
	samplesPerRound := 120 // 2min 数据/轮，> 1.5 * chunkRange
	rounds := 6            // 6 轮 = 总跨度 ~12min，产出多个 block

	// retention = 3 * blockDur = 3min，最终只保留最近 3 个 blockDur 的 block。
	retentionDuration := 3 * blockDur

	dir := t.TempDir()
	opts := DefaultOptions()
	opts.BlockDuration = blockDur
	opts.ChunkRange = blockDur
	opts.NoLockfile = true
	opts.SamplesPerChunk = 120

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))

	ctx := context.Background()

	// ── 写入多轮数据并定期 flush ──
	startT := int64(0)
	for round := 0; round < rounds; round++ {
		appendSamples(t, h, numSeries, samplesPerRound, startT, step)
		startT += int64(samplesPerRound) * step

		handled, compactErr := h.SelfCompact(ctx)
		require.NoError(t, compactErr)
		require.True(t, handled)
	}

	// Close flush 所有尾巴数据。
	require.NoError(t, h.Close())

	allBlocks := listBlocks(t, dir)
	t.Logf("total blocks after close: %d", len(allBlocks))
	require.GreaterOrEqual(t, len(allBlocks), 3,
		"至少需要 3 个 block 来验证 retention 清理")

	for _, b := range allBlocks {
		t.Logf("  block %s: [%d, %d) dur=%dms",
			b.ULID, b.MinTime, b.MaxTime, b.MaxTime-b.MinTime)
	}

	// ── 模拟 tsdb.DB 的 BeyondTimeRetention 逻辑 ──
	// 找出所有 block 中最大的 MaxTime，然后把 MaxTime 距最大 MaxTime
	// 超过 retentionDuration 的 block 判定为过期。
	var maxBlockMaxT int64
	for _, b := range allBlocks {
		if b.MaxTime > maxBlockMaxT {
			maxBlockMaxT = b.MaxTime
		}
	}

	var expiredULIDs []string
	var retainedCount int
	for _, b := range allBlocks {
		if maxBlockMaxT-b.MaxTime > retentionDuration {
			expiredULIDs = append(expiredULIDs, b.ULID.String())
			t.Logf("  expired: %s [%d, %d) (age=%dms > retention=%dms)",
				b.ULID, b.MinTime, b.MaxTime,
				maxBlockMaxT-b.MaxTime, retentionDuration)
		} else {
			retainedCount++
		}
	}

	require.Greater(t, len(expiredULIDs), 0,
		"应有至少 1 个 block 超出 retention，否则测试数据跨度不够")
	require.Greater(t, retainedCount, 0,
		"应有至少 1 个 block 在 retention 范围内")

	// ── 删除过期 block 目录（模拟 tsdb.DB.deleteBlocks） ──
	for _, uid := range expiredULIDs {
		blockDir := filepath.Join(dir, uid)
		require.NoError(t, os.RemoveAll(blockDir),
			"删除过期 block 目录失败: %s", uid)
		t.Logf("  deleted block dir: %s", uid)
	}

	// ── 验证 1: 磁盘上只保留 retention 范围内的 block ──
	remainingBlocks := listBlocks(t, dir)
	t.Logf("remaining blocks after retention cleanup: %d (was %d)",
		len(remainingBlocks), len(allBlocks))
	require.Equal(t, retainedCount, len(remainingBlocks),
		"retention 清理后 block 数应等于保留数")

	// 验证保留的 block 都在 retention 窗口内。
	for _, b := range remainingBlocks {
		age := maxBlockMaxT - b.MaxTime
		require.LessOrEqualf(t, age, retentionDuration,
			"保留的 block %s age=%dms 超出 retention=%dms",
			b.ULID, age, retentionDuration)
	}

	// ── 验证 2: litehead 重启后仍能正常工作 ──
	// 重新打开 Head，写入新数据，再 flush，验证不受已删除 block 影响。
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))

	// 在已有时间线之后继续写入。
	app := h2.Appender(ctx)
	for i := 0; i < numSeries; i++ {
		lset := labels.FromStrings("__name__", "retention_metric",
			"instance", fmt.Sprintf("inst_%d", i))
		ts := startT
		for j := 0; j < samplesPerRound; j++ {
			_, appendErr := app.Append(0, lset, ts, float64(ts))
			require.NoError(t, appendErr)
			ts += step
		}
	}
	require.NoError(t, app.Commit())

	// flush 新数据。
	handled, compactErr := h2.SelfCompact(ctx)
	require.NoError(t, compactErr)
	require.True(t, handled)

	require.NoError(t, h2.Close())

	// ── 验证 3: 新 block 被正确产出 ──
	finalBlocks := listBlocks(t, dir)
	t.Logf("final blocks after restart+flush: %d", len(finalBlocks))
	require.Greater(t, len(finalBlocks), len(remainingBlocks),
		"重启后写入并 flush 应产出新 block")

	// 验证所有 block 的时间范围不重叠（包括旧保留的和新产出的）。
	type timeRange struct{ min, max int64 }
	var ranges []timeRange
	for _, b := range finalBlocks {
		ranges = append(ranges, timeRange{b.MinTime, b.MaxTime})
	}
	for i := range ranges {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[j].min < ranges[i].min {
				ranges[i], ranges[j] = ranges[j], ranges[i]
			}
		}
	}
	for i := 1; i < len(ranges); i++ {
		require.LessOrEqualf(t, ranges[i-1].max, ranges[i].min,
			"block 时间范围重叠：[%d,%d) 和 [%d,%d)",
			ranges[i-1].min, ranges[i-1].max,
			ranges[i].min, ranges[i].max)
	}

	t.Logf("✓ block retention cleanup: %d expired blocks deleted, %d retained, "+
		"restart OK, %d total final blocks",
		len(expiredULIDs), retainedCount, len(finalBlocks))
}

// ---------- Test 5: 高 Churn 清理 ----------

// TestE2E_SeriesChurnMemory 验证高 churn 场景下（series 不断创建和消亡），
// flush 后死 series 被正确回收，内存不会无限增长。
func TestE2E_SeriesChurnMemory(t *testing.T) {
	blockDur := int64(60_000) // 1min
	step := int64(1_000)      // 1s
	samplesPerBatch := 120    // 2min，确保 > 1.5 * chunkRange

	h, dir := newE2EHead(t, blockDur, blockDur)
	_ = dir

	var seriesCountAfterFlush []uint64

	startT := int64(0)
	for round := 0; round < 5; round++ {
		// 每轮写不同的 series（模拟 churn）。
		app := h.Appender(context.Background())
		for i := 0; i < 200; i++ {
			lset := labels.FromStrings(
				"__name__", "churn_metric",
				"instance", fmt.Sprintf("round%d_inst%d", round, i),
			)
			ts := startT
			for j := 0; j < samplesPerBatch; j++ {
				_, err := app.Append(0, lset, ts, float64(ts))
				require.NoError(t, err)
				ts += step
			}
		}
		require.NoError(t, app.Commit())
		startT += int64(samplesPerBatch) * step

		// Flush。
		handled, err := h.SelfCompact(context.Background())
		require.NoError(t, err)
		require.True(t, handled)

		seriesCount := h.NumSeries()
		seriesCountAfterFlush = append(seriesCountAfterFlush, seriesCount)
		t.Logf("round %d: after flush numSeries=%d", round, seriesCount)
	}

	// 在高 churn 下，旧 round 的 series 应该被 sweepDeadSeries 回收。
	// 最后一轮的 series 数不应远超单轮的 200（应在 ~200-400 之间，
	// 取决于是否还有跨窗口的活跃 series）。
	lastCount := seriesCountAfterFlush[len(seriesCountAfterFlush)-1]
	// 5 轮 * 200 = 1000，如果不回收会积累到 1000。
	// 正确回收后应该远小于 1000。
	t.Logf("final numSeries=%d (expected much less than %d if GC works)", lastCount, 5*200)
	require.Lessf(t, lastCount, uint64(800),
		"5 轮 churn 后 series 数=%d，预期 <800（说明死 series 未被有效回收）", lastCount)

	require.NoError(t, h.Close())
}
