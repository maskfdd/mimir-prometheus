// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package litehead

// ----------------------------------------------------------------------------
// litehead 基础 benchmark 集（规划文档 §5 性能指标要求的三组）：
//
//   - BenchmarkLiteHeadAppendRefPath
//       衡量 ref 快路径单次 Append 的 CPU 与分配。这是写入侧 hot path
//       的直接信号，是 PR-2（mmappedChunks 瘦身）的主要收益点。
//
//   - BenchmarkLiteHeadFlushBlockReader
//       衡量 `newBlockReader + Index + Chunks + Close/done` 整条 flush
//       出口的开销。是 PR-3（pooled snapshot scratch）与 PR-5（两级
//       arena，sliceLocked 去复制）的合并收益点。
//
//   - BenchmarkLiteHeadSnapshotWriteAndLoad
//       衡量 snapshot 端到端（write + read back）耗时，直接覆盖 PR-5
//       snapshot / replay 并行化收益。通过子 benchmark 方式遍历不同的
//       WALReplayConcurrency 取值，便于直观看出并行度对耗时的影响。
//
// 使用方式：
//
//   go test -bench=BenchmarkLiteHead -benchmem -benchtime=2s -count=3 \
//       ./tsdb/litehead/...
//
// 规划明确要求关注 `alloc/op`、`B/op`、flush duration、replay duration。
// 用 -benchmem 即可一次输出上述核心指标，-count=3 做稳定性过滤。
// ----------------------------------------------------------------------------

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// newBenchHead 与 newTestHead 语义等价，但入口是 *testing.B；同时默认放大
// ChunkRange / BlockDuration，避免 benchmark 过程中自动产生 flush（这会把
// 纯写路径开销与 flush 开销混在一起，失去测量意义）。
func newBenchHead(b *testing.B, opts *Options) (*Head, string) {
	b.Helper()
	dir := b.TempDir()
	if opts == nil {
		opts = DefaultOptions()
	}
	// 宽 chunkRange：避免 benchmark 期间触发 maybeCutChunk → sealAndSpill。
	// 这样我们量到的就是 Append 热路径本身，不掺 seal/spill。
	opts.ChunkRange = 60 * 60 * 1000   // 1h
	opts.BlockDuration = 2 * 60 * 60 * 1000 // 2h
	opts.SamplesPerChunk = 120
	opts.NoLockfile = true

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(b, err)
	require.NoError(b, h.Init(math.MinInt64))
	return h, dir
}

// newBenchHeadForFlush 配置窄 ChunkRange，让预置数据能真正产出多个 sealed
// chunk，flush benchmark 的 newBlockReader 路径才会有工作量。
func newBenchHeadForFlush(b *testing.B) (*Head, string) {
	b.Helper()
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.ChunkRange = 60 * 1000       // 1min
	opts.BlockDuration = 60 * 1000    // 1min
	opts.SamplesPerChunk = 8
	opts.NoLockfile = true

	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(b, err)
	require.NoError(b, h.Init(math.MinInt64))
	return h, dir
}

// genBenchLabels 构造 numSeries 条互不相同的 labels；保持 label 数量稳定
// （3 对），模拟中等基数的 metric。hash 分布受 instance/host 决定。
func genBenchLabels(numSeries int) []labels.Labels {
	out := make([]labels.Labels, numSeries)
	for i := 0; i < numSeries; i++ {
		out[i] = labels.FromStrings(
			"__name__", fmt.Sprintf("metric_%05d", i%1024),
			"host", fmt.Sprintf("host-%d", i),
			"job", "bench",
		)
	}
	return out
}

// -----------------------------------------------------------------------------
// BenchmarkLiteHeadAppendRefPath
// -----------------------------------------------------------------------------
//
// 先用标签路径 Append 拿到 ref，再在稳态循环里只走 ref 快路径。
// 这样量到的是 litehead 在稳态写入下的 per-sample 代价（核心热点）。
//
// 设计细节：
//   - 每条 series 初始化阶段跑在 timer 重置之前
//   - benchmark 内层循环只对单条 series 累加时间戳，避免每次都走 hashIdx
//     的快路径分支（如果也测多 series 分布，另起 benchmark）
//   - 用固定的 t 增量 1（毫秒），单次 Append 自然成为 in-order sample，
//     不会触发 OOO 路径
func BenchmarkLiteHeadAppendRefPath(b *testing.B) {
	cases := []struct {
		name   string
		series int
	}{
		{"series=1", 1},
		{"series=1k", 1_000},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			h, _ := newBenchHead(b, nil)
			defer h.Close()

			ctx := context.Background()
			lsets := genBenchLabels(tc.series)
			refs := make([]storage.SeriesRef, tc.series)

			// 预建 series：labels 路径一次，后续全走 ref 快路径。
			app := h.Appender(ctx)
			for i, lset := range lsets {
				ref, err := app.Append(0, lset, int64(1000+i), float64(i))
				if err != nil {
					b.Fatalf("initial append: %v", err)
				}
				refs[i] = ref
			}
			if err := app.Commit(); err != nil {
				b.Fatalf("initial commit: %v", err)
			}

			// 每轮迭代用一个独立 appender，模拟 Mimir ingester 的
			// per-request appender 模式（否则会长期持有单个 appender，
			// 与真实语义不符）。
			baseTs := int64(100_000)

			b.ReportAllocs()
			b.ResetTimer()

			// i 递增作为 t 增量；series 维度做 round-robin 避免只热一条。
			for i := 0; i < b.N; i++ {
				a := h.Appender(ctx)
				// 每个 appender 写一小批 samples，贴近实际
				// （Mimir push 一次往往是几十到几百条）。
				const batch = 16
				for k := 0; k < batch; k++ {
					idx := (i*batch + k) % tc.series
					ts := baseTs + int64(i*batch+k)
					if _, err := a.Append(refs[idx], labels.EmptyLabels(), ts, float64(k)); err != nil {
						b.Fatalf("append: %v", err)
					}
				}
				if err := a.Commit(); err != nil {
					b.Fatalf("commit: %v", err)
				}
			}
			// 每次迭代写 batch 条样本；把 b.N 归一化到 "per-sample" 才有
			// 横向比较意义。这里不用 b.SetBytes（没有明确 B/op 维度），
			// 通过调用方以 (ns/op)/batch 自行换算即可。
		})
	}
}

// -----------------------------------------------------------------------------
// BenchmarkLiteHeadFlushBlockReader
// -----------------------------------------------------------------------------
//
// 预置 N 条 series × 若干 sealed chunks，然后在稳态循环里反复构造
// blockReader，打开 Index/Chunks 再关闭。
//
// 这条路径对应规划 §4.2 / §4.3 / §5.2：
//   - `symbolSet` 构造：PR-3 直接从 symbolTable 拿，不再全量 decode
//   - open chunk snapshot：PR-3 pooled scratch
//   - labelCat.get 路径在 Index 构造里被 decodeLabels 大量调用：PR-5
//     两级 arena 的 sliceLocked 去复制直接影响这里
//
// 我们不实际调 Flush 出 block（那会涉及 ChunkDiskMapper 落盘 IO，
// benchmark 噪声大），只量到 newBlockReader + Index/Chunks open/close
// 这一段；这正是 flush 的"纯计算 + in-memory" 部分。
func BenchmarkLiteHeadFlushBlockReader(b *testing.B) {
	cases := []struct {
		name   string
		series int
	}{
		{"series=1k", 1_000},
		{"series=10k", 10_000},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			h, _ := newBenchHeadForFlush(b)
			defer h.Close()

			ctx := context.Background()
			lsets := genBenchLabels(tc.series)

			// 预置数据：每条 series 写若干 sample，跨越多个 chunkRange 边界
			// 以产出 sealed chunks。ChunkRange=60s、SamplesPerChunk=8，
			// 每 180s 至少 2~3 次 seal。
			app := h.Appender(ctx)
			for i, lset := range lsets {
				// 每条 series 30 个样本；时间跨度 ~300s，能产出若干 sealed chunk。
				for k := 0; k < 30; k++ {
					ts := int64(1000 + k*10_000)
					if _, err := app.Append(0, lset, ts, float64(i+k)); err != nil {
						b.Fatalf("prep append: %v", err)
					}
					// 只对第一条样本走标签路径；后续让 ref 复用……不行，这里每条 series 都不同，
					// 先保持简单：全部走 labels 路径，成本都预置阶段吃掉。
					_ = lset
				}
			}
			if err := app.Commit(); err != nil {
				b.Fatalf("prep commit: %v", err)
			}

			// 锁定要 flush 的窗口：用整个 Head 覆盖的时间范围。
			mint := h.MinTime()
			maxt := h.MaxTime()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				br := newBlockReader(h, mint, maxt)
				ir, err := br.Index()
				if err != nil {
					b.Fatalf("index: %v", err)
				}
				cr, err := br.Chunks()
				if err != nil {
					b.Fatalf("chunks: %v", err)
				}
				if err := ir.Close(); err != nil {
					b.Fatalf("ir.Close: %v", err)
				}
				if err := cr.Close(); err != nil {
					b.Fatalf("cr.Close: %v", err)
				}
				// newBlockReader 自身持有 1 个 openReaders 计数，Index/Chunks 各 +1；
				// Close 各 -1，最后 br.done() 释放初始计数归还 pool buffer。
				br.done()
			}
		})
	}
}

// -----------------------------------------------------------------------------
// BenchmarkLiteHeadSnapshotWriteAndLoad
// -----------------------------------------------------------------------------
//
// 预置 N 条 series → close 触发 writeSnapshot → 重开触发 loadSnapshot
// 全流程耗时。这是 PR-5（snapshot/replay 并行化）的直接验收 benchmark。
//
// 子 benchmark 用 WALReplayConcurrency=1/4 两组，串行与并行对比；
// 运行时日志里的 "workers" 字段也能直观反映实际生效的并发度。
//
// 备注：benchmark 的 b.N 代表一次"写 + 读"的完整循环；为了让单次 iter
// 耗时不随 N 抖动，每个 iter 都建新目录、新 Head。这引入目录创建开销，
// 但相对 snapshot IO 来说可以忽略。
func BenchmarkLiteHeadSnapshotWriteAndLoad(b *testing.B) {
	cases := []struct {
		name        string
		series      int
		concurrency int
	}{
		{"series=5k/workers=1", 5_000, 1},
		{"series=5k/workers=4", 5_000, 4},
		{"series=50k/workers=1", 50_000, 1},
		{"series=50k/workers=4", 50_000, 4},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			lsets := genBenchLabels(tc.series)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// --- 写阶段 ---
				opts := DefaultOptions()
				opts.ChunkRange = 60 * 1000
				opts.BlockDuration = 60 * 1000
				opts.SamplesPerChunk = 8
				opts.NoLockfile = true
				opts.EnableMemorySnapshotOnShutdown = true
				opts.WALReplayConcurrency = tc.concurrency

				dir := b.TempDir()
				h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
				if err != nil {
					b.Fatalf("NewHead: %v", err)
				}
				if err := h.Init(math.MinInt64); err != nil {
					b.Fatalf("Init: %v", err)
				}

				app := h.Appender(ctx)
				for j, lset := range lsets {
					if _, err := app.Append(0, lset, int64(1000+j), float64(j)); err != nil {
						b.Fatalf("append: %v", err)
					}
				}
				if err := app.Commit(); err != nil {
					b.Fatalf("commit: %v", err)
				}

				// Close 触发 snapshot。
				if err := h.Close(); err != nil {
					b.Fatalf("close (writeSnapshot): %v", err)
				}

				// --- 读阶段 ---
				opts2 := *opts // 值拷贝；不共享 Options 状态，避免 NewHead 改写。
				h2, err := NewHead(log.NewNopLogger(), nil, dir, &opts2)
				if err != nil {
					b.Fatalf("NewHead reopen: %v", err)
				}
				// Init 会调 loadSnapshot。
				if err := h2.Init(math.MinInt64); err != nil {
					b.Fatalf("Init reopen: %v", err)
				}
				if got := h2.NumSeries(); got != uint64(tc.series) {
					b.Fatalf("reopen: series count mismatch: want=%d got=%d", tc.series, got)
				}
				if err := h2.Close(); err != nil {
					b.Fatalf("close after reopen: %v", err)
				}
			}
		})
	}
}
