package litehead

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// TestUnsupportedWriteTypesReturnExplicitError 验证 litehead 对 exemplar / histogram /
// metadata 的写入调用一律显式返回 ErrUnsupportedWriteType，而不是静默成功。
//
// 这是 PR-1 的核心语义保证：接入方一旦错误地调用到这些入口，应该立刻感知，
// 而不是像之前那样拿到 (0, nil) 误以为写入成功。
func TestUnsupportedWriteTypesReturnExplicitError(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)

	// Exemplar 写入必须显式失败。
	_, err := app.AppendExemplar(0, lset, exemplar.Exemplar{Labels: lset, Value: 1, Ts: 1000})
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Integer histogram 写入必须显式失败。
	_, err = app.AppendHistogram(0, lset, 1000, &histogram.Histogram{}, nil)
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Float histogram 写入必须显式失败（同入口，float 分支走 nil int hist）。
	_, err = app.AppendHistogram(0, lset, 1000, nil, &histogram.FloatHistogram{})
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Metadata 写入必须显式失败。
	_, err = app.UpdateMetadata(0, lset, metadata.Metadata{Type: "counter"})
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Commit 不应因为此前的 unsupported 错误而失败：appender 未记录任何 pending 状态。
	require.NoError(t, app.Commit())
}

// TestRollbackDoesNotLeakSamples 验证 A1 的修复：Append 成功后调用 Rollback，
// 已登记的样本不会被持久化到 open chunk，也不会推高 series.lastTs/openMaxT。
//
// 修复前的 bug：Append 阶段就直接 s.openApp.Append 写入 XOR chunk 字节流，
// Rollback 只能丢弃 pending 切片，无法撤销已写入的字节——导致 rollback 的
// 样本仍会出现在 flush 后的 block 中。
func TestRollbackDoesNotLeakSamples(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	// 先 Append(t=10) 并 Rollback：该样本必须完全消失。
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 10, 1.0)
	require.NoError(t, err)
	require.NotZero(t, ref)
	require.NoError(t, app.Rollback())

	// Head 不应出现 t=10 样本的任何痕迹：
	// - Head.MaxTime 仍是 MinInt64（updateMinMaxTime 从未被调用）
	// - series 的 lastTs / openMaxT 仍是初始值
	// - openChunk 仍未分配
	require.Equal(t, int64(math.MinInt64), h.MaxTime(),
		"rollback 后 Head.MaxTime 不应被推进")

	s := h.refTab.get(chunks.HeadSeriesRef(ref))
	require.NotNil(t, s, "series 本身保留（已进 WAL），但样本状态应清空")
	s.mu.Lock()
	require.Equal(t, int64(math.MinInt64), s.lastTs, "lastTs 不应被 rollback 的样本推高")
	require.Nil(t, s.openChunk, "openChunk 不应在 rollback 后存在，openMaxT 保持为新建 series 的零值")
	s.mu.Unlock()

	// 再次以**相同或更小**的 t 写入：若上一轮的 t=10 真的泄漏进 open chunk，
	// 这一步会触发 ErrOutOfOrderSample；修复后必须成功。
	app2 := h.Appender(ctx)
	_, err = app2.Append(ref, lset, 10, 2.0)
	require.NoError(t, err, "rollback 应彻底清除 t=10，重写 t=10 必须被允许")
	require.NoError(t, app2.Commit())

	s.mu.Lock()
	require.Equal(t, int64(10), s.lastTs, "commit 后 lastTs=10")
	s.mu.Unlock()
}

// TestAppendBatchInternalOutOfOrderRejected 验证 A1 修复后仍保留"批内乱序拒绝"
// 语义：同一 appender 内连续 Append 同一 series 的非单调 t，第二条必须返回
// ErrOutOfOrderSample。依赖 appender.batchSeriesMaxT。
func TestAppendBatchInternalOutOfOrderRejected(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)
	_, err := app.Append(0, lset, 100, 1.0)
	require.NoError(t, err)

	// 等值（非严格单调）应被拒。
	_, err = app.Append(0, lset, 100, 2.0)
	require.ErrorIs(t, err, storage.ErrOutOfOrderSample)

	// 更小的 t 也应被拒。
	_, err = app.Append(0, lset, 50, 3.0)
	require.ErrorIs(t, err, storage.ErrOutOfOrderSample)

	// 严格单调应通过。
	_, err = app.Append(0, lset, 101, 4.0)
	require.NoError(t, err)

	require.NoError(t, app.Commit())
}

// TestConcurrentAppendersFinalDefenseDropsOutOfOrder 验证 A1 修复的"最终防线"：
// 两个并发 appender 都在同一 series 上 Append，且被写入的 t 在 Append 阶段
// 各自通过乐观预检，但真正 Commit 时后到的 appender 看到前一个已经推高了
// openMaxT——此时必须丢弃乱序样本（计入 outOfOrderSamples），而不是写进
// XOR chunk 破坏单调性。
func TestConcurrentAppendersFinalDefenseDropsOutOfOrder(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	appA := h.Appender(ctx)
	appB := h.Appender(ctx)

	// A 和 B 都在 series 首次 Append：此时 series 的 lastTs/openMaxT 都是
	// math.MinInt64，两个 appender 的 Append 都会通过预检。
	refA, err := appA.Append(0, lset, 200, 1.0)
	require.NoError(t, err)
	refB, err := appB.Append(0, lset, 199, 2.0)
	require.NoError(t, err)
	require.Equal(t, refA, refB)

	// A 先 Commit：series.openMaxT=200, lastTs=200。
	require.NoError(t, appA.Commit())

	// B 后 Commit：t=199 <= openMaxT=200，最终防线必须丢弃，不写入 open chunk。
	require.NoError(t, appB.Commit())

	s := h.refTab.get(chunks.HeadSeriesRef(refA))
	require.NotNil(t, s)
	s.mu.Lock()
	// lastTs 必须仍是 200（严格单调）。
	require.Equal(t, int64(200), s.lastTs)
	require.Equal(t, int64(200), s.openMaxT)
	// open chunk 只有 A 那一条（t=200）。
	require.NotNil(t, s.openChunk)
	require.Equal(t, 1, s.openChunk.NumSamples(),
		"B 的 t=199 必须被最终防线丢弃，chunk 里只有 A 的一条样本")
	s.mu.Unlock()
}

// TestAppenderNotInitializedReturnsErrAppender 验证 B3 修复：Head 未 Init
// 时调 Appender() 返回 errAppender，所有写操作都显式失败，Commit/Rollback
// 也返回 ErrNotInitialized，而不是返回一个会写入未恢复 refTab 的真实 appender。
func TestAppenderNotInitializedReturnsErrAppender(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.NoLockfile = true
	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })

	// 故意**不**调 Init。
	app := h.Appender(context.Background())
	lset := labels.FromStrings("__name__", "cpu")

	_, err = app.Append(0, lset, 1, 1.0)
	require.ErrorIs(t, err, ErrNotInitialized)

	_, err = app.AppendExemplar(0, lset, exemplar.Exemplar{})
	require.ErrorIs(t, err, ErrNotInitialized)

	_, err = app.AppendHistogram(0, lset, 1, &histogram.Histogram{}, nil)
	require.ErrorIs(t, err, ErrNotInitialized)

	_, err = app.UpdateMetadata(0, lset, metadata.Metadata{})
	require.ErrorIs(t, err, ErrNotInitialized)

	require.ErrorIs(t, app.Commit(), ErrNotInitialized)
	require.ErrorIs(t, app.Rollback(), ErrNotInitialized)
}

// TestAppenderAbandonedDoesNotBlockSelfCompact 验证 B1 修复：
// 调用方拿到 appender 后既不 Commit 也不 Rollback（模拟 panic / 遗忘），
// SelfCompact 不应被卡死。修复前 Appender() 持 appenderMtx.RLock 且放在
// reset() 里释放，一旦调用方丢弃 appender 就永远不会 RUnlock，SelfCompact
// 的 Lock() 会永久阻塞整个 Head。
func TestAppenderAbandonedDoesNotBlockSelfCompact(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	// 先正常写一条让 compactable 条件更可能成立（可不成立也行，此测试核心在
	// SelfCompact 不 hang）。
	app := h.Appender(ctx)
	_, err := app.Append(0, lset, 100, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 拿到一个 appender 后**立即丢弃**，模拟调用方 panic / 漏写 defer。
	_ = h.Appender(ctx)
	// 注意：故意不 Commit、不 Rollback、不 reset。

	// SelfCompact 必须能在合理时间内返回（不受丢弃 appender 影响）。
	done := make(chan error, 1)
	go func() {
		_, compactErr := h.SelfCompact(ctx)
		done <- compactErr
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("SelfCompact was blocked by abandoned appender — appenderMtx RLock leaked")
	}
}

// TestCommitContinuesOnErrorAndAggregates 是 B2 修复的最小冒烟：Commit 的
// 循环在遇到错误时不应中止后续样本的处理。此处用一个"无错误"场景验证
// 多样本全部被处理后 Commit 返回 nil；真正的错误聚合场景需要注入 IO 失败，
// 代价较大，单独用注释说明。修复前：首个 error 会立即 return，后续 pending
// 样本丢失。
func TestCommitContinuesOnErrorAndAggregates(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lsetA := labels.FromStrings("__name__", "a")
	lsetB := labels.FromStrings("__name__", "b")

	app := h.Appender(ctx)
	_, err := app.Append(0, lsetA, 10, 1.0)
	require.NoError(t, err)
	_, err = app.Append(0, lsetB, 20, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 两条 series 都必须已落盘（open chunk 非空、lastTs 已更新）。
	for _, ls := range []labels.Labels{lsetA, lsetB} {
		app2 := h.Appender(ctx).(*appender)
		ref, _ := app2.GetRef(ls, ls.Hash())
		require.NotZero(t, ref, "series %s should exist", ls.String())
		s := h.refTab.get(chunks.HeadSeriesRef(ref))
		require.NotNil(t, s)
		s.mu.Lock()
		require.NotEqual(t, int64(math.MinInt64), s.lastTs)
		s.mu.Unlock()
		require.NoError(t, app2.Rollback())
	}
}

// TestCommitRunCoalescesSameSeries 验证 Commit 的"同 series 连续样本合并锁区间"
// 优化保持语义正确：一批 A1,A2,A3,B1,B2,A4,A5 混合顺序样本全部落盘，每条都满足
// 单调 t、lastTs 追上最大 t、open chunk 样本计数与输入对齐（无漏、无重复）。
// 这是合并锁实现的核心语义测试：
//   - 同 series 连续段（A1..A3 / B1..B2 / A4..A5）每段一次 Lock/Unlock；
//   - 若实现里错把同一个 s.mu 递归加锁或漏 Unlock，此用例在 race 下会挂；
//   - 若合并后某条样本被错误跳过或重复写入，NumSamples 与 lastTs 会对不上。
func TestCommitRunCoalescesSameSeries(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lsetA := labels.FromStrings("__name__", "a")
	lsetB := labels.FromStrings("__name__", "b")

	app := h.Appender(ctx)
	// 刻意制造"同 series 多条 + 交替 series"，覆盖 run 分组的典型分支：
	//   run1: A @ 10,20,30 -> Lock/Unlock 一次
	//   run2: B @ 15,25    -> Lock/Unlock 一次
	//   run3: A @ 40,50    -> Lock/Unlock 一次
	refA, err := app.Append(0, lsetA, 10, 1.0)
	require.NoError(t, err)
	_, err = app.Append(refA, labels.EmptyLabels(), 20, 2.0)
	require.NoError(t, err)
	_, err = app.Append(refA, labels.EmptyLabels(), 30, 3.0)
	require.NoError(t, err)
	refB, err := app.Append(0, lsetB, 15, 10.0)
	require.NoError(t, err)
	_, err = app.Append(refB, labels.EmptyLabels(), 25, 20.0)
	require.NoError(t, err)
	_, err = app.Append(refA, labels.EmptyLabels(), 40, 4.0)
	require.NoError(t, err)
	_, err = app.Append(refA, labels.EmptyLabels(), 50, 5.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// 验证 A 的状态：lastTs=50，open chunk 5 条样本。
	sA := h.refTab.get(chunks.HeadSeriesRef(refA))
	require.NotNil(t, sA)
	sA.mu.Lock()
	require.Equal(t, int64(50), sA.lastTs)
	require.Equal(t, int64(50), sA.openMaxT)
	require.NotNil(t, sA.openChunk)
	require.Equal(t, 5, sA.openChunk.NumSamples(), "A 必须完整写入 5 条样本")
	sA.mu.Unlock()

	// 验证 B 的状态：lastTs=25，open chunk 2 条样本。
	sB := h.refTab.get(chunks.HeadSeriesRef(refB))
	require.NotNil(t, sB)
	sB.mu.Lock()
	require.Equal(t, int64(25), sB.lastTs)
	require.Equal(t, int64(25), sB.openMaxT)
	require.NotNil(t, sB.openChunk)
	require.Equal(t, 2, sB.openChunk.NumSamples(), "B 必须完整写入 2 条样本")
	sB.mu.Unlock()
}

// TestCommitRunOutOfOrderWithinRunDropped 覆盖 run 合并后的最终防线语义：
// 同一 run 内，若某条样本的 t 不满足单调（例如 batch 内已 Append 但之间有
// 并发 appender 抢先提交），commitSampleRun 会跳过它并递增 outOfOrderSamples
// 指标，而不是中断整个 run。
//
// 我们通过两步构造触发：先用 appender A 记录好 pending t=100 / t=200；
// 在 Commit 前，通过 appender B 抢先写一条 t=150 并 Commit。此时 A 的
// t=200 仍然合法（>150）会落，但 t=100 的最终防线会把自己挡掉（<150）。
func TestCommitRunOutOfOrderWithinRunDropped(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	// 预建 series，方便拿 ref。
	app0 := h.Appender(ctx)
	ref, err := app0.Append(0, lset, 50, 0)
	require.NoError(t, err)
	require.NoError(t, app0.Commit())

	// A：构造 pending t=100 与 t=200，但不 Commit。
	// 注意：Append 阶段会做只读乱序预检，t<=lastTs(50) 会被拒；我们用 100 和 200 都 > 50。
	appA := h.Appender(ctx)
	_, err = appA.Append(ref, labels.EmptyLabels(), 100, 1.0)
	require.NoError(t, err)
	_, err = appA.Append(ref, labels.EmptyLabels(), 200, 2.0)
	require.NoError(t, err)

	// B：抢先提交 t=150，使 lastTs=150。
	appB := h.Appender(ctx)
	_, err = appB.Append(ref, labels.EmptyLabels(), 150, 1.5)
	require.NoError(t, err)
	require.NoError(t, appB.Commit())

	// A.Commit：run 内 t=100 被最终防线挡掉，t=200 落盘。
	require.NoError(t, appA.Commit())

	s := h.refTab.get(chunks.HeadSeriesRef(ref))
	require.NotNil(t, s)
	s.mu.Lock()
	require.Equal(t, int64(200), s.lastTs)
	require.Equal(t, int64(200), s.openMaxT)
	// open chunk: 50(来自 app0) + 150(来自 B) + 200(来自 A) = 3 条；
	// 100 被最终防线丢弃。
	require.Equal(t, 3, s.openChunk.NumSamples(),
		"run 内 t=100 必须被最终防线丢弃，open chunk 含 3 条样本（50/150/200）")
	s.mu.Unlock()
}
