package litehead

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

// newTestHeadAt 在指定目录打开一个测试用 Head，便于在同一目录上模拟
// "关机 -> 重启" 的 crash recovery 路径。
func newTestHeadAt(t *testing.T, dir string, opts *Options) *Head {
	t.Helper()
	if opts == nil {
		opts = DefaultOptions()
		opts.ChunkRange = 60 * 1000
		opts.BlockDuration = 60 * 1000
		opts.SamplesPerChunk = 8
		opts.NoLockfile = true
	}
	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))
	return h
}

// countBlocks 返回 dir 下符合 block 目录命名（26 字符 ULID）的目录数。
func countBlocks(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			n++
		}
	}
	return n
}

// sumBlockSamples 统计 dir 下所有 block 的总样本数。
func sumBlockSamples(t *testing.T, dir string) uint64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var total uint64
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != 26 {
			continue
		}
		b, err := tsdb.OpenBlock(nil, filepath.Join(dir, e.Name()), nil)
		require.NoError(t, err)
		total += b.Meta().Stats.NumSamples
		require.NoError(t, b.Close())
	}
	return total
}

// TestCrashAfterCommitBeforeFlushRecoverable 验证 litehead 当前 crash recovery
// 的 **真实语义**：Commit 成功但尚未 flush 就 crash 时，
//
//  1. series 索引（ref / labels / lastTs）必须能被 WAL replay 还原；
//  2. 重启后用同一 labels 应该复用原 ref；
//  3. 重启后继续写入的新样本，在 flush 后必须完整落盘。
//
// **已知语义边界（engineering_optimization_plan.md Phase 0 前置项）**：
// 当前 WAL replay 只恢复 `lastTs` 等索引状态，不会把 crash 前 Commit 的样本
// 重新塞回 open chunk。因此 crash 前的样本**不会**出现在 flush 后的 block 里。
// 这是 write-only 语义下的预期行为，但必须有显式测试兜底——未来一旦改成
// 恢复 open chunk，这个测试会自动变红，提醒更新语义断言。
func TestCrashAfterCommitBeforeFlushRecoverable(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true

	dir := t.TempDir()

	// 第一次打开：写两条样本后只 Commit，不 flush。
	h1, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h1.Init(math.MinInt64))

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")
	app := h1.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	_, err = app.Append(ref, labels.EmptyLabels(), 2000, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 模拟 crash：直接关闭底层 WAL + CDM，但不经过 Close() 的 flush 流程。
	// 这样 WAL 内容落盘，但 block 尚未生成。
	require.NoError(t, h1.chunkDiskMapper.Close())
	require.NoError(t, h1.wal.Close())
	require.NoError(t, h1.locker.Release())

	// 崩溃前应该尚未产生任何 block。
	require.Equal(t, 0, countBlocks(t, dir), "no block expected before first flush")

	// 重启：WAL replay 应该恢复 series 索引；同 labels 写入必须复用原 ref。
	h2 := newTestHeadAt(t, dir, opts)

	app2 := h2.Appender(ctx).(*appender)
	gotRef, _ := app2.GetRef(lset, lset.Hash())
	require.Equal(t, ref, gotRef, "series must be recovered with original ref")

	// 崩溃后继续写入两条样本，时间戳必须严格递增（litehead 是 in-order only）。
	_, err = app2.Append(gotRef, labels.EmptyLabels(), 3000, 3.0)
	require.NoError(t, err)
	_, err = app2.Append(gotRef, labels.EmptyLabels(), 4000, 4.0)
	require.NoError(t, err)
	require.NoError(t, app2.Commit())

	// 全量 flush：当前语义下，只有 crash 后写入的 2 条样本会出现在 block。
	// crash 前 Commit 的 2 条样本会丢失——这是已知的 P0 前置项，不是 bug，是未做的工作。
	require.NoError(t, h2.Close())

	require.GreaterOrEqual(t, countBlocks(t, dir), 1, "expected at least one block after recovery + flush")
	require.Equal(t, uint64(2), sumBlockSamples(t, dir),
		"current litehead replay only restores series index, not open-chunk samples; "+
			"if this number ever goes up, update the assertion to match the new recovery semantics")
}

// TestRestartAfterFlushContinuesNewSamples 验证：正常关机（Close 会 flush）后，
// 重启再写一批样本、再 flush，数据不会因为 minValidTime/WAL 不一致而被静默拒绝。
//
// 这覆盖规划里 "重启后继续写再 flush" 这一条。
func TestRestartAfterFlushContinuesNewSamples(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true

	dir := t.TempDir()

	h1, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h1.Init(math.MinInt64))

	ctx := context.Background()
	lsetA := labels.FromStrings("__name__", "a")

	app := h1.Appender(ctx)
	_, err = app.Append(0, lsetA, 1000, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 正常关机：会触发 tryFlushAll，把窗口内数据刷成 block。
	require.NoError(t, h1.Close())
	firstBlocks := countBlocks(t, dir)
	require.GreaterOrEqual(t, firstBlocks, 1)

	// 重启后继续写到**下一个 block 窗口**，避免落回已 flush 过的时间段被 minValidTime 拒绝。
	h2 := newTestHeadAt(t, dir, opts)
	app2 := h2.Appender(ctx)
	_, err = app2.Append(0, lsetA, 2*opts.BlockDuration, 42.0)
	require.NoError(t, err)
	require.NoError(t, app2.Commit())

	require.NoError(t, h2.Close())

	// 应至少新增 1 个 block，且总样本数 = 第一次 1 条 + 第二次 1 条。
	require.Greater(t, countBlocks(t, dir), firstBlocks,
		"expected a new block after restart + append + flush")
	require.Equal(t, uint64(2), sumBlockSamples(t, dir),
		"all committed samples across restarts must land in some block")
}

// TestForcedFlushThenNormalFlushInterleave 验证：单条 series 触发 forced flush 之后，
// 紧接着的正常 flush 仍然能把剩余样本写成 block，不丢数据、也不会出现重复。
//
// 这覆盖规划里 "forced flush 与正常 flush 混跑" 这一条。
func TestForcedFlushThenNormalFlushInterleave(t *testing.T) {
	opts := DefaultOptions()
	// 每条样本都切新 chunk，很快触发 maxMmappedChunksPerSeries 的 forced flush。
	opts.SamplesPerChunk = 1
	opts.ChunkRange = 10
	opts.BlockDuration = 60 * 1000
	opts.NoLockfile = true

	dir := t.TempDir()
	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "force")

	// 第一批：写 3 * max，必然触发 forced flush。
	const firstBatch = int64(maxMmappedChunksPerSeries * 3)
	app := h.Appender(ctx)
	var ref storage.SeriesRef
	for i := int64(0); i < firstBatch; i++ {
		ref, err = app.Append(ref, lset, 1000+i*opts.ChunkRange*2, float64(i))
		require.NoError(t, err, "forced-flush path must not drop samples; failed at i=%d", i)
	}
	require.NoError(t, app.Commit())

	// 第二批：继续写更晚的时间戳，确保 forced flush 后写路径仍然正常。
	const secondBatch = int64(maxMmappedChunksPerSeries)
	startT := 1000 + firstBatch*opts.ChunkRange*2
	app = h.Appender(ctx)
	for i := int64(0); i < secondBatch; i++ {
		_, err = app.Append(ref, labels.EmptyLabels(), startT+i*opts.ChunkRange*2, 1000+float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	// Close 触发最后一次正常 flush。
	require.NoError(t, h.Close())

	require.Equal(t, uint64(firstBatch+secondBatch), sumBlockSamples(t, dir),
		"forced flush + normal flush must together persist every committed sample exactly once")
}

// TestOutOfBoundsAfterFlush 验证：flush 后 minValidTime 会推进，重启前写入更老时间戳
// 必须被 storage.ErrOutOfBounds 明确拒绝，而不是静默成功。
//
// 这条是 "不支持写入类型 / 语义边界" 测试矩阵里的一条重要正向样本。
func TestOutOfBoundsAfterFlush(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true

	dir := t.TempDir()
	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)
	_, err = app.Append(0, lset, 3*opts.BlockDuration, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.NoError(t, h.tryFlushAll())

	// flush 过后 minValidTime 会被推进；写一个远早于已 flush 窗口的时间戳必须被拒绝。
	app2 := h.Appender(ctx)
	_, err = app2.Append(0, lset, 1000, 2.0)
	require.ErrorIs(t, err, storage.ErrOutOfBounds,
		"samples older than flushed window must be rejected, not silently accepted")
	require.NoError(t, app2.Commit())
}
