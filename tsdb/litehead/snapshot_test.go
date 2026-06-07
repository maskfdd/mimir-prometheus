package litehead

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
)

func TestEncodeLiteSnapshotRecord(t *testing.T) {
	ref := chunks.HeadSeriesRef(42)
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")
	lastTs := int64(123456)

	// 编码
	buf := encodeLiteSnapshotRecord(nil, ref, lset, lastTs)

	// 解码
	gotRef, gotLset, gotLastTs, err := decodeLiteSnapshotRecord(buf)
	require.NoError(t, err)
	require.Equal(t, ref, gotRef)
	require.True(t, labels.Equal(lset, gotLset))
	require.Equal(t, lastTs, gotLastTs)
}

func TestEncodeLiteSnapshotRecordMinInt64LastTs(t *testing.T) {
	ref := chunks.HeadSeriesRef(1)
	lset := labels.FromStrings("__name__", "test")
	lastTs := int64(math.MinInt64)

	buf := encodeLiteSnapshotRecord(nil, ref, lset, lastTs)
	gotRef, gotLset, gotLastTs, err := decodeLiteSnapshotRecord(buf)
	require.NoError(t, err)
	require.Equal(t, ref, gotRef)
	require.True(t, labels.Equal(lset, gotLset))
	require.Equal(t, lastTs, gotLastTs)
}

func TestLiteSnapshotDirNaming(t *testing.T) {
	name := liteSnapshotDir(42, 12345)
	require.Equal(t, "chunk_snapshot.000042.0000012345", name)

	// 验证 lastLiteSnapshot 能找到它。
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, name), 0o777))
	path, idx, offset, err := lastLiteSnapshot(dir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, name), path)
	require.Equal(t, 42, idx)
	require.Equal(t, 12345, offset)
}

func TestLastLiteSnapshotSelectsLatest(t *testing.T) {
	dir := t.TempDir()
	// 创建多个 snapshot 目录。
	require.NoError(t, os.MkdirAll(filepath.Join(dir, liteSnapshotDir(1, 100)), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, liteSnapshotDir(3, 200)), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, liteSnapshotDir(2, 500)), 0o777))

	_, idx, offset, err := lastLiteSnapshot(dir)
	require.NoError(t, err)
	require.Equal(t, 3, idx)
	require.Equal(t, 200, offset)
}

func TestLastLiteSnapshotNotFound(t *testing.T) {
	dir := t.TempDir()
	_, _, _, err := lastLiteSnapshot(dir)
	require.ErrorIs(t, err, record.ErrNotFound)
}

func TestDeleteLiteSnapshots(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, liteSnapshotDir(1, 100)), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, liteSnapshotDir(2, 200)), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, liteSnapshotDir(3, 300)), 0o777))

	// 删除比 (3, 300) 旧的。
	require.NoError(t, deleteLiteSnapshots(dir, 3, 300))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	// 只剩最新的。
	snapDirs := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), liteSnapshotPrefix) {
			snapDirs++
		}
	}
	require.Equal(t, 1, snapDirs)
}

// TestSnapshotWriteAndLoad 验证：关机时写出 snapshot，重新打开时通过 snapshot
// 恢复 series 和 lastTs，而不是完整 replay WAL。
func TestSnapshotWriteAndLoad(t *testing.T) {
	opts := DefaultOptions()
	opts.EnableMemorySnapshotOnShutdown = true
	h, dir := newTestHead(t, opts)

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")

	// 写入一些数据。
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	_, err = app.Append(ref, labels.EmptyLabels(), 2000, 2.0)
	require.NoError(t, err)
	_, err = app.Append(ref, labels.EmptyLabels(), 3000, 3.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 确认数据写入成功。
	require.Equal(t, uint64(1), h.NumSeries())
	require.Equal(t, int64(1000), h.MinTime())
	require.Equal(t, int64(3000), h.MaxTime())

	// 正常关机（会写 snapshot）。
	require.NoError(t, h.Close())

	// 检查 snapshot 目录存在。
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	foundSnapshot := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), liteSnapshotPrefix) && e.IsDir() {
			foundSnapshot = true
		}
	}
	require.True(t, foundSnapshot, "expected chunk_snapshot directory to be created on close")

	// 重新打开。
	opts = DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	opts.EnableMemorySnapshotOnShutdown = true
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))
	t.Cleanup(func() { _ = h2.Close() })

	// 验证 series 通过 snapshot 恢复。
	appender2 := h2.Appender(ctx).(*appender)
	gotRef, _ := appender2.GetRef(lset, lset.Hash())
	require.NotZero(t, gotRef, "series must be recovered from snapshot")
	require.Equal(t, ref, gotRef, "ref must match the original")
	require.NoError(t, appender2.Commit())

	// 确认 lastTs 被恢复：如果写入一个时间戳 <= 3000 的样本，应该被拒绝。
	app = h2.Appender(ctx)
	_, err = app.Append(gotRef, labels.EmptyLabels(), 3000, 4.0)
	require.ErrorIs(t, err, storage.ErrOutOfOrderSample)
	require.NoError(t, app.Commit())

	// 写入一个更新的时间戳应该可以成功。
	app = h2.Appender(ctx)
	_, err = app.Append(gotRef, labels.EmptyLabels(), 4000, 4.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
}

// TestSnapshotWithMultipleSeries 验证多条 series 都能被 snapshot 正确恢复。
func TestSnapshotWithMultipleSeries(t *testing.T) {
	opts := DefaultOptions()
	opts.EnableMemorySnapshotOnShutdown = true
	h, dir := newTestHead(t, opts)

	ctx := context.Background()
	lsetA := labels.FromStrings("__name__", "a", "host", "h1")
	lsetB := labels.FromStrings("__name__", "b", "host", "h2")
	lsetC := labels.FromStrings("__name__", "c", "host", "h3")

	app := h.Appender(ctx)
	refA, err := app.Append(0, lsetA, 1000, 1.0)
	require.NoError(t, err)
	refB, err := app.Append(0, lsetB, 2000, 2.0)
	require.NoError(t, err)
	refC, err := app.Append(0, lsetC, 3000, 3.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.Equal(t, uint64(3), h.NumSeries())
	require.NoError(t, h.Close())

	// 重新打开。
	opts = DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	opts.EnableMemorySnapshotOnShutdown = true
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))
	t.Cleanup(func() { _ = h2.Close() })

	// 验证所有 series 都被恢复。
	appender := h2.Appender(ctx).(*appender)
	gotA, _ := appender.GetRef(lsetA, lsetA.Hash())
	require.Equal(t, refA, gotA)
	gotB, _ := appender.GetRef(lsetB, lsetB.Hash())
	require.Equal(t, refB, gotB)
	gotC, _ := appender.GetRef(lsetC, lsetC.Hash())
	require.Equal(t, refC, gotC)
	require.NoError(t, appender.Commit())

	require.Equal(t, uint64(3), h2.NumSeries())
}

// TestSnapshotPlusIncrementalWAL 验证：snapshot 加载之后，snapshot 之后的增量
// WAL 也能被正确 replay。
func TestSnapshotPlusIncrementalWAL(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	opts.EnableMemorySnapshotOnShutdown = true
	dir := t.TempDir()

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	// 阶段 1：写一些数据，然后关机（写 snapshot）。
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, h.Close())

	// 阶段 2：重新打开，写更多数据，然后不正常退出（模拟 crash）。
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))

	app = h2.Appender(ctx)
	_, err = app.Append(ref, labels.EmptyLabels(), 5000, 5.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 不调用 Close()，直接关闭底层资源模拟 crash。
	h2.chunkDiskMapper.Close()
	h2.wal.Close()
	if h2.locker != nil {
		h2.locker.Release()
	}

	// 阶段 3：再次打开。snapshot 恢复阶段1的 series，
	// 增量 WAL 恢复阶段2的 sample。
	h3, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h3.Init(math.MinInt64))
	t.Cleanup(func() { _ = h3.Close() })

	// series 应该被恢复。
	appender := h3.Appender(ctx).(*appender)
	gotRef, _ := appender.GetRef(lset, lset.Hash())
	require.Equal(t, ref, gotRef)
	require.NoError(t, appender.Commit())

	// lastTs 应该是 5000（来自增量 WAL replay）。
	ws := h3.refTab.get(chunks.HeadSeriesRef(ref))
	require.NotNil(t, ws)
	require.Equal(t, int64(5000), ws.lastTs)
}

// TestSnapshotFlushAndReopen 验证完整生命周期：写数据 -> flush -> 关机（snapshot）
// -> 重新打开 -> 写更多数据 -> flush -> 验证 block。
func TestSnapshotFlushAndReopen(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	opts.EnableMemorySnapshotOnShutdown = true
	dir := t.TempDir()

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")

	// 写数据并关机。
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	_, err = app.Append(ref, labels.EmptyLabels(), 2000, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, h.Close())

	// 重新打开并写更多数据到第二个 block 窗口。
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))

	app = h2.Appender(ctx)
	_, err = app.Append(ref, labels.EmptyLabels(), 70_000, 7.0) // 新窗口
	require.NoError(t, err)
	_, err = app.Append(ref, labels.EmptyLabels(), 80_000, 8.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 全量 flush 并关机。
	require.NoError(t, h2.Close())

	// 检查至少产生了一个 block。
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	blockCount := 0
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != 26 {
			continue
		}
		b, err := tsdb.OpenBlock(nil, filepath.Join(dir, e.Name()), nil)
		require.NoError(t, err)
		require.NoError(t, b.Close())
		blockCount++
	}
	require.GreaterOrEqual(t, blockCount, 1, "at least one block should have been flushed")
}

// TestNoSnapshotFallbackToWALReplay 验证：如果没有 snapshot，启动时仍然
// 能正常通过 WAL replay 恢复。
func TestNoSnapshotFallbackToWALReplay(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	dir := t.TempDir()

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 不调用 Close，直接关底层资源（模拟 crash，不会产生 snapshot）。
	h.chunkDiskMapper.Close()
	h.wal.Close()
	if h.locker != nil {
		h.locker.Release()
	}

	// 重新打开，应该通过 WAL replay 恢复。
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))
	t.Cleanup(func() { _ = h2.Close() })

	appender := h2.Appender(ctx).(*appender)
	gotRef, _ := appender.GetRef(lset, lset.Hash())
	require.Equal(t, ref, gotRef, "series should be recovered from WAL replay")
	require.NoError(t, appender.Commit())
}
