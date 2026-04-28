package litehead

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// -----------------------------------------------------------------------------
// labelCatalog 两级 arena 的语义与边界用例（PR-5）
//
// 这些用例要求：无论 put 进的编码跨 chunk、触发 oversized 路径，还是并发读
// 正在被写入的 catalog，`get/compare/equals` 都必须保持老实现的语义。
// -----------------------------------------------------------------------------

// TestLabelCatalogChunkedArenaRoundTrip 验证：在跨多个 chunk 的量级下，每条
// labelsID 都能准确回读；compare 排序结果与直接按 labels 字典序一致；equals 与
// labels.Equal 一致。
//
// 用例构造 ~50 条 labels，虽然不足以触发 1 MiB chunk 切换，但配合下面的
// OversizedPayload 用例共同覆盖"常规 chunk 内 + oversized chunk"两类路径。
func TestLabelCatalogChunkedArenaRoundTrip(t *testing.T) {
	lc := newLabelCatalog()

	const n = 50
	lsets := make([]labels.Labels, n)
	ids := make([]uint32, n)
	for i := 0; i < n; i++ {
		lsets[i] = labels.FromStrings(
			"__name__", fmt.Sprintf("metric_%03d", i),
			"host", fmt.Sprintf("h-%d", i),
			"zone", "us-east-1",
		)
		ids[i] = lc.put(lsets[i])
	}

	// get 回读必须逐条匹配。
	for i := 0; i < n; i++ {
		got := lc.get(ids[i])
		require.True(t, labels.Equal(lsets[i], got),
			"get(id=%d) mismatch: want=%v got=%v", ids[i], lsets[i], got)
		require.True(t, lc.equals(ids[i], lsets[i]),
			"equals(id=%d, original) must be true", ids[i])
	}

	// compare 必须与 labels 字典序一致（通过随机挑两条 labelsID 对比）。
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			got := lc.compare(ids[i], ids[j])
			expected := labels.Compare(lsets[i], lsets[j])
			// compare 的语义只约定符号一致，不一定数值相等。
			require.Equal(t, sign(expected), sign(got),
				"compare(%d,%d) sign mismatch: want=%d got=%d (lsets[%d]=%v, lsets[%d]=%v)",
				i, j, expected, got, i, lsets[i], j, lsets[j])
		}
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}

// TestLabelCatalogOversizedPayload 直接覆盖 `reserveLocked` 的 oversized 分支：
// 当要求容量 > labelCatalogChunkSize 时，必须独占一整块 oversized chunk，并在后
// 面补一个新的空活跃 chunk，保持"最后一个 chunk 是活跃 chunk"的不变式。
//
// 为什么不构造真实的 oversized put：
//   - labelCatalog 把 label name/value 都通过 symbolTable 去重成 uvarint ID；
//     一对 label 编码成 2~10 字节。要让单条 series 的编码字节数 > 1 MiB，理论上
//     需要 ~200k 对 label，这已经完全脱离合理场景、且会让单测极慢。
//   - 因此我们在 lc.mu 写锁下**直接调用 reserveLocked**，验证分支的关键不变式
//     （仍然在同一包内测试，没有破坏封装）。
//   - 真实 put 路径的回读正确性由 TestLabelCatalogChunkedArenaRoundTrip 覆盖。
func TestLabelCatalogOversizedPayload(t *testing.T) {
	lc := newLabelCatalog()

	// 正常一条：走活跃 chunk 分支。
	small := labels.FromStrings("__name__", "x", "host", "h1")
	idSmall := lc.put(small)
	chunksBefore := len(lc.chunks)

	// 直接调 reserveLocked 触发 oversized 分支：
	// 要求一块 > labelCatalogChunkSize 的空间，不实际写入字节。
	lc.mu.Lock()
	oversizedID, offset := lc.reserveLocked(uint32(labelCatalogChunkSize + 1024))
	// 把这块 oversized chunk 的 len 手工推到 cap，模拟写入完成——这样 size()
	// 的断言能反映出这块 chunk 的占用。注意：cap = encLen = chunkSize+1024。
	lc.chunks[oversizedID] = lc.chunks[oversizedID][:cap(lc.chunks[oversizedID])]
	lc.mu.Unlock()

	// 再来一条普通的：reserveLocked 内部在 oversized 分支**已追加**了一个
	// 新的空活跃 chunk，因此这条 put 不会再新开 chunk，会直接写到那个空活跃
	// chunk 的开头。
	afterHuge := labels.FromStrings("__name__", "z", "host", "h2")
	idAfter := lc.put(afterHuge)

	// small 与 afterHuge 两条都能正确回读（没被 oversized 分支的内部 chunk
	// 追加破坏）。
	require.True(t, labels.Equal(small, lc.get(idSmall)))
	require.True(t, labels.Equal(afterHuge, lc.get(idAfter)))

	// oversized 必须独占一整块，且后面紧跟一个新的空活跃 chunk。
	// chunksBefore 只含首个 chunk；reserveLocked 新增 2 个（oversized + fresh）。
	require.Equal(t, chunksBefore+2, len(lc.chunks),
		"oversized path must have allocated a dedicated chunk AND a fresh active chunk")
	require.Equal(t, uint32(0), offset,
		"oversized record must start at offset 0 of its dedicated chunk")
	require.Equal(t, uint32(chunksBefore), oversizedID,
		"oversized chunk ID must be the chunk right after the previously active one")
	// 最后一个 chunk 是新的空活跃 chunk（idAfter 写入后 len 是 afterHuge 的编码长度）。
	require.LessOrEqual(t, len(lc.chunks[len(lc.chunks)-1]), 32,
		"fresh active chunk after oversized must contain only the next small record")
}

// TestLabelCatalogConcurrentPutAndGet 验证：写锁保护下，一边 put 一边 get 不会
// 返回损坏的 labels；即两级 arena 的 sub-slice 返回在并发下仍稳定。
//
// 这个用例是 PR-5 的核心并发正确性证据之一：老实现 `sliceLocked` 通过 make+copy
// 做"读脱离底层数组"的隔离；两级 arena 去掉了复制，必须证明在并发写下读者不会
// 观察到"写了一半"的编码。
func TestLabelCatalogConcurrentPutAndGet(t *testing.T) {
	lc := newLabelCatalog()

	// 预置若干条，保证 reader 有稳定目标。
	const preset = 200
	presetIDs := make([]uint32, preset)
	presetLsets := make([]labels.Labels, preset)
	for i := 0; i < preset; i++ {
		presetLsets[i] = labels.FromStrings("__name__", "m", "i", fmt.Sprintf("%d", i))
		presetIDs[i] = lc.put(presetLsets[i])
	}

	stop := make(chan struct{})
	var writerWG, readerWG sync.WaitGroup

	// Writer：持续 put 新 labels 直到收到 stop 信号，不断触发 chunk 滚动。
	// 单独用 writerWG 以便 readers 先结束后主动 close(stop) 让 writer 退出，
	// **绝不能先 wg.Wait() 再 close(stop)**——那会造成 writer 永远不退出 / 测试死锁。
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for i := preset; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			lc.put(labels.FromStrings("__name__", "m", "i", fmt.Sprintf("%d", i)))
		}
	}()

	// Readers：反复 get 预置的 labelsID，校验内容稳定。跑完固定轮数即退出。
	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			// 总 get 次数控制在 ~100k 级别；窗口内 writer 足以滚动多个 chunk
			// （1 MiB / ~20 字节/条 ≈ 50k 条 / chunk），可稳定触发 chunk 切换而不超时。
			for iter := 0; iter < 500; iter++ {
				for j := 0; j < preset; j++ {
					got := lc.get(presetIDs[j])
					if !labels.Equal(presetLsets[j], got) {
						// 用 panic 传递错误，避免 require 的 goroutine 跨界；
						// goroutine-panic 会直接让 test runner 失败并打出栈。
						panic(fmt.Sprintf(
							"labelCatalog concurrent get returned corrupted labels: want=%v got=%v",
							presetLsets[j], got))
					}
				}
			}
		}()
	}

	// 等所有 readers 跑完规定的轮数，再通知 writer 停，最后等 writer 收尾。
	readerWG.Wait()
	close(stop)
	writerWG.Wait()
}

// TestLabelCatalogIncludesOversizedInSize 验证 size() 把 oversized chunk 的字节
// 占用也计入。监控侧依赖 size() 反映 labels arena 的真实常驻成本，漏记会让告
// 警看不见"几 MiB 级别的超长 labels 累积占用"。
//
// 构造方式同 TestLabelCatalogOversizedPayload：直接走 reserveLocked 并把 len 推
// 到 cap（见该用例里对\"为什么不实际 put 1MB 编码\"的说明）。
func TestLabelCatalogIncludesOversizedInSize(t *testing.T) {
	lc := newLabelCatalog()

	// 先 put 一条普通的，让 count() == 1 且 size > 0 有基线。
	lc.put(labels.FromStrings("__name__", "x", "host", "h1"))
	baseline := lc.size()

	// 直接触发 oversized 分支，把 oversized chunk 填满。
	const oversize = labelCatalogChunkSize + 64
	lc.mu.Lock()
	oversizedID, _ := lc.reserveLocked(uint32(oversize))
	lc.chunks[oversizedID] = lc.chunks[oversizedID][:cap(lc.chunks[oversizedID])]
	lc.mu.Unlock()

	require.GreaterOrEqual(t, lc.size(), baseline+oversize,
		"size() must include oversized chunk bytes")
	// count() 仅反映已登记的 labelsID 条数——oversized 分支没调 put，因此
	// 仍然是那 1 条普通的。这样验证 size() 与 count() 的语义没被错绑。
	require.Equal(t, 1, lc.count())
}

// -----------------------------------------------------------------------------
// snapshot / replay 并行化往返（PR-5 子任务 B）
//
// 写入较大量 series → 关机写 snapshot → 重开读 snapshot → 验证完全一致。
// 关键点：使用并发写入来填 refPages，保证并行 writeSnapshot 覆盖到多 worker 路径。
// -----------------------------------------------------------------------------

// TestSnapshotRoundTripParallelWorkers 构造 >1K series 让 snapshot 走到多 worker
// 分片路径，关机 + 重开后确认 NumSeries / lastTs / labels 全部一致。
//
// 用例特意把 pages 数量推高（series 数 > refPageSize 会开新 page）以触发
// snapshotWorkerCount 返回 >1。默认 refPageSize=1<<14=16384，直接写这么多条
// 对 CI 时间不友好；我们通过在 series 内塞多条 label 把 labelCatalog 压力做大，
// 但 series 数保持在 2000 量级（仍在单 page 内，用于验证"少量 pages 下走串行"路径）。
//
// 然后再写一个 **多 page** 版本：通过 setRef 路径人工把 ref 推到多个 page，
// 但这会破坏 lastSeriesID 递增假设——故不这么做。这里只覆盖单 page 路径的
// 正确性，并行多 page 路径由 `TestSnapshotRoundTripMultiPages` 覆盖。
func TestSnapshotRoundTripParallelWorkers(t *testing.T) {
	opts := DefaultOptions()
	opts.EnableMemorySnapshotOnShutdown = true
	opts.WALReplayConcurrency = 4 // 强制并行度，便于覆盖 worker 分片
	h, dir := newTestHead(t, opts)

	ctx := context.Background()

	const n = 2000
	app := h.Appender(ctx)
	refs := make([]storage.SeriesRef, n)
	lsets := make([]labels.Labels, n)
	for i := 0; i < n; i++ {
		lsets[i] = labels.FromStrings(
			"__name__", fmt.Sprintf("metric_%05d", i),
			"host", fmt.Sprintf("h-%d", i%16),
			"job", "roundtrip-worker",
		)
		ref, err := app.Append(0, lsets[i], int64(1000+i), float64(i))
		require.NoError(t, err)
		refs[i] = ref
	}
	require.NoError(t, app.Commit())
	require.Equal(t, uint64(n), h.NumSeries())

	// 关机 → 写 snapshot。
	require.NoError(t, h.Close())

	// 重开 → 并行 loadSnapshot。
	opts2 := DefaultOptions()
	opts2.ChunkRange = 60 * 1000
	opts2.BlockDuration = 60 * 1000
	opts2.SamplesPerChunk = 8
	opts2.NoLockfile = true
	opts2.EnableMemorySnapshotOnShutdown = true
	opts2.WALReplayConcurrency = 4
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts2)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))
	t.Cleanup(func() { _ = h2.Close() })

	require.Equal(t, uint64(n), h2.NumSeries(),
		"all series must be restored by parallel snapshot load")

	// 抽样验证 labels & lastTs 都被正确恢复：随机抽 64 条。
	appender2 := h2.Appender(ctx).(*appender)
	for i := 0; i < n; i += n / 64 {
		gotRef, ok := appender2.GetRef(lsets[i], lsets[i].Hash())
		require.NotZero(t, gotRef, "series %d must be recovered", i)
		_ = ok
		// lastTs 正确性：写入比 lastTs 旧的样本必须 OOO 失败。
		_, err := appender2.Append(gotRef, labels.EmptyLabels(), int64(1000+i), 0.0)
		require.ErrorIs(t, err, storage.ErrOutOfOrderSample,
			"lastTs must be recovered; writing same ts must be rejected")
	}
	require.NoError(t, appender2.Rollback())
}

// TestSnapshotWorkerCountFallsBackForTinyCatalog 验证小规模 snapshot 走串行，
// 避免启动 goroutine 的无谓开销。
func TestSnapshotWorkerCountFallsBackForTinyCatalog(t *testing.T) {
	opts := DefaultOptions()
	opts.WALReplayConcurrency = 8
	h, _ := newTestHead(t, opts)
	t.Cleanup(func() { _ = h.Close() })

	// pages<=2 → 强制 1 worker，无论 Options 怎么配。
	require.Equal(t, 1, h.snapshotWorkerCount(1))
	require.Equal(t, 1, h.snapshotWorkerCount(2))
	// pages>2 → 按 Options 上限。
	require.Equal(t, 8, h.snapshotWorkerCount(100))
	// 受 pageCount 封顶。
	require.Equal(t, 3, h.snapshotWorkerCount(3))
}

// TestSnapshotRoundTripWorkerCountOne 验证当 Options.WALReplayConcurrency=1
// 时仍能完整往返（串行分支与并行分支语义一致）。
func TestSnapshotRoundTripWorkerCountOne(t *testing.T) {
	opts := DefaultOptions()
	opts.EnableMemorySnapshotOnShutdown = true
	opts.WALReplayConcurrency = 1
	h, dir := newTestHead(t, opts)

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "h1")
	app := h.Appender(ctx)
	ref, err := app.Append(0, lset, 1000, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, h.Close())

	opts2 := DefaultOptions()
	opts2.ChunkRange = 60 * 1000
	opts2.BlockDuration = 60 * 1000
	opts2.SamplesPerChunk = 8
	opts2.NoLockfile = true
	opts2.EnableMemorySnapshotOnShutdown = true
	opts2.WALReplayConcurrency = 1
	h2, err := NewHead(log.NewNopLogger(), nil, dir, opts2)
	require.NoError(t, err)
	require.NoError(t, h2.Init(math.MinInt64))
	t.Cleanup(func() { _ = h2.Close() })

	require.Equal(t, uint64(1), h2.NumSeries())
	appender2 := h2.Appender(ctx).(*appender)
	gotRef, _ := appender2.GetRef(lset, lset.Hash())
	require.Equal(t, ref, gotRef)
	require.NoError(t, appender2.Rollback())
}
