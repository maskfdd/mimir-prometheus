package litehead

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

func newTestHead(t *testing.T, opts *Options) (*Head, string) {
	t.Helper()
	dir := t.TempDir()
	if opts == nil {
		opts = DefaultOptions()
	}
	// 测试用较短的 chunkRange / block range，避免等太久。
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true

	h, err := Open(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	return h, dir
}

func TestWriteAndReadBackViaFlush(t *testing.T) {
	h, dir := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	require.NotZero(t, ref)
	// 复用 ref
	_, err = app.Append(ref, labels.EmptyLabels(), 2000, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 再写一批
	app = h.Appender(ctx)
	_, err = app.Append(ref, labels.EmptyLabels(), 3000, 3.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.Equal(t, int64(1000), h.MinTime())
	require.Equal(t, int64(3000), h.MaxTime())
	require.Equal(t, 1, h.NumSeries())

	// 强制 flush 窗口 [0, BlockDuration-1]，写出 block。
	require.NoError(t, h.tryFlushAll())

	// 应该至少出现一个 block 目录（ULID 26 字符）。
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) == 26 {
			found = true
			// 校验 block 内容：用 tsdb.OpenBlock 打开并读取样本数。
			b, err := tsdb.OpenBlock(nil, filepath.Join(dir, name), nil)
			require.NoError(t, err)
			meta := b.Meta()
			require.GreaterOrEqual(t, meta.Stats.NumSeries, uint64(1))
			require.GreaterOrEqual(t, meta.Stats.NumSamples, uint64(3))
			require.NoError(t, b.Close())
		}
	}
	require.True(t, found, "expected at least one flushed block directory")
}

func TestRefReuseHot(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")

	app := h.Appender(ctx)
	ref1, err := app.Append(0, lset, 100, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 用同样的 ref 再写一次，确保热路径可用。
	app = h.Appender(ctx)
	ref2, err := app.Append(ref1, labels.EmptyLabels(), 200, 2)
	require.NoError(t, err)
	require.Equal(t, ref1, ref2)
	require.NoError(t, app.Commit())

	// GetRef 也应能命中。
	appender := h.Appender(ctx).(*appender)
	gotRef, gotLset := appender.GetRef(lset, lset.Hash())
	require.Equal(t, ref1, gotRef)
	require.True(t, labels.Equal(lset, gotLset))
	require.NoError(t, appender.Commit())
}

func TestOutOfOrderSampleRejected(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1)
	require.NoError(t, err)
	_, err = app.Append(ref, labels.EmptyLabels(), 500, 2)
	require.ErrorIs(t, err, storage.ErrOutOfOrderSample)
	require.NoError(t, app.Commit())
}

func TestRestartWALReplayRecoversSeries(t *testing.T) {
	h, dir := newTestHead(t, nil)

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 模拟关机
	require.NoError(t, h.Close())

	// 重新打开
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	h2, err := Open(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h2.Close() })

	// 现在用相同 labels 做 GetRef 应该能拿到原来的 ref（或至少 series 已存在）。
	appender2 := h2.Appender(ctx).(*appender)
	gotRef, _ := appender2.GetRef(lset, lset.Hash())
	require.NotZero(t, gotRef, "series must be recovered from WAL")
	require.Equal(t, ref, gotRef)
	require.NoError(t, appender2.Commit())
}

func TestChunkSealingOnCapacity(t *testing.T) {
	opts := DefaultOptions()
	opts.SamplesPerChunk = 2 // 让 chunk 切分频繁发生
	h, _ := newTestHead(t, opts)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")
	app := h.Appender(ctx)
	var ref storage.SeriesRef
	var err error
	// 写 20 条样本，应该至少触发 1 次 chunk 切分。
	for i := int64(0); i < 20; i++ {
		ref, err = app.Append(ref, lset, 1000+i, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
}

func TestRollbackStillLogsNewSeries(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	require.NotZero(t, ref)
	require.NoError(t, app.Rollback())

	// series 应该依然可用（Rollback 也会写 Series 记录到 WAL）。
	appender := h.Appender(ctx).(*appender)
	gotRef, _ := appender.GetRef(lset, lset.Hash())
	require.Equal(t, ref, gotRef)
	require.NoError(t, appender.Commit())
}

// TestSealedOverflowDoesNotLoseData 验证：即使 mmappedChunks[] 数组达到容量上限，
// append 也不会被拒绝，更不会丢弃任何已 commit 的样本。
//
// 构造方式：SamplesPerChunk=1 + 强制 ChunkRange 极短，让每条样本都会切 chunk
// 并 spill 出一个 mmapped chunk；写入远多于 maxMmappedChunksPerSeries*1 条样本后，
// 所有样本必须可以在 flush 出 block 后被完整读回。
func TestSealedOverflowDoesNotLoseData(t *testing.T) {
	opts := DefaultOptions()
	// 每条样本都会触发 chunk 切分 -> sealed++
	opts.SamplesPerChunk = 1
	opts.ChunkRange = 10           // 10ms 就滚一个 chunk
	opts.BlockDuration = 60 * 1000 // 让 block 足够大，把所有样本都落到同一个 block
	opts.NoLockfile = true
	dir := t.TempDir()
	h, err := Open(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "hsealed")

	// 写 3 倍 maxMmappedChunksPerSeries 数量的样本，确保至少发生一次 mmapped chunks 超限。
	const total = int64(maxMmappedChunksPerSeries * 3)
	app := h.Appender(ctx)
	var ref storage.SeriesRef
	for i := int64(0); i < total; i++ {
		// 每条样本时间戳递增 ChunkRange*2，保证一定切新 chunk 并 spill。
		ref, err = app.Append(ref, lset, 1000+i*opts.ChunkRange*2, float64(i))
		require.NoError(t, err, "append at i=%d must not fail when mmapped chunks overflow", i)
	}
	require.NoError(t, app.Commit())

	// 触发一次关机路径的全量 flush，把所有样本落成 block。
	require.NoError(t, h.Close())

	// 扫描 block 目录，把所有 block 的样本数加起来应等于 total。
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var totalSamples uint64
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != 26 {
			continue
		}
		b, err := tsdb.OpenBlock(nil, filepath.Join(dir, e.Name()), nil)
		require.NoError(t, err)
		totalSamples += b.Meta().Stats.NumSamples
		require.NoError(t, b.Close())
	}
	require.Equal(t, uint64(total), totalSamples,
		"all committed samples must be flushed to blocks; none allowed to be dropped")
}

// TestGCAfterFlushRemovesIdleSeries 验证：flush 成功后，窗口之外已经
// 不再被写入的 series（无 openChunk、无 sealed[]、lastTs <= flushMaxt）
// 会被 GC 从 refTable / hashIndex 中剔除；NumSeries 会相应下降。
func TestGCAfterFlushRemovesIdleSeries(t *testing.T) {
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true
	h, _ := func() (*Head, string) {
		dir := t.TempDir()
		d, err := Open(log.NewNopLogger(), nil, dir, opts)
		require.NoError(t, err)
		return d, dir
	}()
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	// 创建两条 series，落在同一个 flush 窗口内。
	lsetA := labels.FromStrings("__name__", "a")
	lsetB := labels.FromStrings("__name__", "b")

	app := h.Appender(ctx)
	refA, err := app.Append(0, lsetA, 1000, 1)
	require.NoError(t, err)
	refB, err := app.Append(0, lsetB, 1500, 2)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.Equal(t, 2, h.NumSeries())

	// 全量 flush，应把 [MinT, MaxT] 全部落成 block。
	require.NoError(t, h.tryFlushAll())

	// flush 后：两条 series 都没有 openChunk、没有 sealed[]、lastTs 已过窗口，
	// 应该都被 GC 清掉。NumSeries 归零。
	require.Equal(t, 0, h.NumSeries(),
		"idle series should be garbage-collected after flush")

	// 同样 labels 再次写入，应该会创建新的 series，不再复用旧 ref。
	app = h.Appender(ctx)
	newRefA, err := app.Append(0, lsetA, 70_000, 3)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NotEqual(t, refA, newRefA, "new series must get a fresh ref after GC")
	_ = refB
}
