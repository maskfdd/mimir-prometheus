# litehead 工程优化执行日志（Implementation Log）

> 对应决策文档：`optimization_notes.md`
> 对应执行计划：`engineering_optimization_plan.md`
>
> 本文档记录 **PR-1 ~ PR-6** 每一轮落地的具体改动、验收数据、以及对后续
> 方向的影响；便于 review 时回溯"**为什么这么改、改了效果如何**"。

---

## 总览

| PR | 主题 | 状态 | 对应规划章节 |
| -- | --- | --- | --- |
| PR-1 | 正确性与语义（unsupported 显式失败 + crash/flush 测试矩阵） | ✅ 落地 | Phase 0 |
| PR-2 | `mmappedChunks` 瘦身（inline + overflow） | ✅ 落地 | §4.1 |
| PR-3 | `symbolSet` 直取 + open chunk snapshot pool | ✅ 落地 | §4.2 / §4.3 |
| PR-4 | forced flush 弱化为兜底 + soft/hard 两级观测 | ✅ 落地 | §5.1 第一步 |
| PR-5 | `labelCatalog` 两级 arena + snapshot/replay 并行化 | ✅ 落地 | §5.2 第一步 + §5.3 |
| PR-6 | benchmark 三件套（性能验收基线） | ✅ 落地 | §5 性能指标 |

---

## PR-4：forced flush 弱化 + 两级观测阈值

### 背景

老实现单条 series 的 `mmappedChunks` 达到 `maxMmappedChunksPerSeries=8` 时，
立即同步调用 `h.flushBlocking()` —— 把单条热点 series 的局部压力放大成**全局
停顿**。在高 churn 场景下这条路径会被频繁触发，放大 Append P99。

### 落地改动

1. **默认阈值大幅抬高**：常量从 `8` 改为 `defaultHardMmappedChunksPerSeries=64`，
   让 forced flush 从"常态触发"降级为"极端兜底"。
2. **引入 soft 告警阈值**：`defaultSoftMmappedChunksPerSeries=12`，越过时只
   `Inc()` 一个 Counter，不做 flush —— 纯观测信号。
3. **可配置**：`Options.ForcedFlushSealedChunks` / `Options.SoftFlushSealedChunks`
   + `validate()` 的合法性回落逻辑（保证 `0 < soft < hard` 且 `hard >= 2`）。
4. **新增 3 个指标**（`metrics.go`）：
   - `prometheus_tsdb_litehead_mmapped_chunks_soft_flush_hits_total`（Counter）
   - `prometheus_tsdb_litehead_mmapped_chunks_hard_limit`（Gauge）
   - `prometheus_tsdb_litehead_mmapped_chunks_soft_limit`（Gauge）
5. `appender.sealAndSpillLocked`：soft 只在 `sealedLen()+1 == softLimit` 当次
   `Inc()`，避免每次 append 都打点；hard 保持原 `flushBlocking` 兜底语义。

### 新增测试（`forced_flush_test.go`）

- `TestForcedFlushTwoLevelWatermarkDefaults`：验证 soft 只在越线时 +1，hard
  只在到达上限时触发，且无数据丢失。
- `TestForcedFlushOptionsValidationFallsBack`：覆盖 3 组非法输入（both zero /
  soft≥hard / 负值），验证 `validate()` 自愈。
- `TestForcedFlushLimitGaugesReflectConfig`：验证启动时 gauge 反映配置。

### 保留事项

litehead 无内部后台 flush goroutine，soft 命中不会触发异步 flush；调用方
（mimir ingester）仍通过外部 `Flush()` 驱动。soft 指标的语义是**提醒运维侧
调节外部 flush 节奏**，不是自动补偿。

---

## PR-5：`labelCatalog` 两级 arena + snapshot/replay 并行化

### 5-A：`labelCatalog` 两级 arena

#### 背景

老实现 `arena []byte` + `index []uint32`，每次 `sliceLocked()` 都
`make + copy` 一份字节切片；`get/compare/equals` 都带一次分配。

#### 落地改动

- 将 arena 拆成 `chunks [][]byte` + `chunkIDs / chunkOffsets / lengths` 三个
  并行数组（避免 struct padding）。
- `put` → `reserveLocked(encLen)` 选择目标 chunk；单条超过 `labelCatalogChunkSize=1MiB`
  时走 **oversized 分支**，独占一整块并紧跟一个新空活跃 chunk，保持"最后
  一个 chunk 是活跃 chunk"的不变式。
- 关键不变式：**活跃 chunk cap 预留到 `labelCatalogChunkSize`，单条 put 在
  未触发滚动前 `append` 不会 grow** —— 保证旧读者手中的 sub-slice 指向
  的底层数组永远不会被替换。这是并发正确性的安全边界，禁止改成"按需扩容 cap"。
- `sliceLocked()` 直接返回 sub-slice，不再复制。

#### 并发正确性证据

- `TestLabelCatalogConcurrentPutAndGet`：4 reader + 1 writer 持续跑 500 轮
  × 200 lset，writer 持续 put 新 labels 触发多轮 chunk 滚动，readers 做 200k+
  次 `get + labels.Equal`；`go test -race` 下干净。

### 5-B：snapshot / replay 并行化

#### 落地改动

- **writeSnapshot**：按 `refPage` **轮询分配**给 worker 并行编码，
  worker 产出 `batch{buf, recs}` 管道；主 goroutine 串行 `cp.Log` 汇聚；
  错误通过 `errCh` 快速短路。
- **loadSnapshot**：先串行从 WAL Reader 读 raw bytes（reader 非并发安全），
  再并行 `decodeLiteSnapshotRecord`，最后串行 `createSeriesWithRef` 合并
  （`refTab/hashIdx/labelCat/lastSeriesID/numSeries` 五个状态必须串行更新）。
- 小规模 snapshot (`pages<=2` 或 `records<=64`) 自动走串行，避免并行开销。
- 并行度来源：`Options.WALReplayConcurrency`；默认 `GOMAXPROCS/2`，上限 8。

#### 正确性证据

- `TestSnapshotRoundTripParallelWorkers`（2000 series + workers=4）：关机写
  snapshot → 重开并行 load → 验证 `NumSeries` 一致 + 抽样 64 条 lastTs
  正确（用"写同时间戳必须 OOO 失败"反证）。
- `TestSnapshotRoundTripWorkerCountOne` / `TestSnapshotWorkerCountFallsBackForTinyCatalog`
  覆盖串行分支与小规模回退路径。

### 正确性修复

- `writeSnapshot` 老实现存在隐蔽 bug：中间 batch flush 失败只 `level.Warn`
  不中断（第 190 行）。并行化版本统一把所有错误走 `errCh`，中途出错立即
  短路；行为语义更严格。

---

## PR-6：benchmark 三件套

### 目的

为规划 §5"性能指标"要求的三组 benchmark 提供代码基线，后续做深度改造
（如 sealed chunk registry 重构）时可以直接对比数据。

### Benchmark 清单（`bench_test.go`）

| Benchmark | 覆盖路径 | 主要验收 PR |
| --- | --- | --- |
| `BenchmarkLiteHeadAppendRefPath` | `Appender().Append(ref=...)` + WAL 写 + `Commit` | PR-2（memSeries 瘦身）|
| `BenchmarkLiteHeadFlushBlockReader` | `newBlockReader + Index/Chunks + Close + done` | PR-3（pooled scratch、symbolTable 直取）+ PR-5-A（sliceLocked 去复制）|
| `BenchmarkLiteHeadSnapshotWriteAndLoad` | close→writeSnapshot + reopen→loadSnapshot 端到端 | PR-5-B（并行化）|

### 实测数据（首次基线，AMD EPYC 7K62 / 16 vCPU / `-benchtime=300ms`）

#### AppendRefPath（稳态）

```
series=1      34 µs/op    33 KB/op    1 alloc/op   (每 iter 写 16 条样本)
series=1k    4.4 µs/op   562  B/op    0 alloc/op   (每 iter 写 16 条样本，round-robin)
```

换算到 per-sample：
- 1 series 场景 ~ 2.1 µs / sample（主要是 appender 构造 + WAL 写开销）
- 1k series 场景 ~ 273 ns / sample（ref 快路径真实热点）

**每次 Append 几乎 0 alloc**（除 WAL buf 复用后剩的开销），验证 PR-2 的
inline+overflow 结构没有引入额外分配。

#### FlushBlockReader（`-benchtime=1x`，不含落盘 IO）

```
series=1k     191 ms/op   169 MB/op   103k allocs
series=10k    587 ms/op   307 MB/op   930k allocs
```

10k/1k 时间比 ~3.1x，allocs 比 ~9x —— allocs 随 series 数量线性增长（正常
behavior：每条 series 都要构造 chunkMeta + lookup），未来如果要进一步
压降，需重构 refIndex 构造逻辑。

#### SnapshotWriteAndLoad

```
series=5k  / workers=1    122 ms/op
series=5k  / workers=4    123 ms/op     # 规模不足以盖并行启动开销
series=50k / workers=1    763 ms/op
series=50k / workers=4    700 ms/op     # 并行缩短 8.3%
```

**结论**：
- 小规模 snapshot 并行化几乎无加速，`snapshotWorkerCount(pageCount<=2)=1`
  的兜底确实必要。
- 大规模 (50k series) 有可见但有限的并行收益（~8%）；剩余瓶颈主要在
  `cp.Log` 串行写和 `createSeriesWithRef` 串行合并。
- 要取得更大加速比，必须进一步重构"状态合并"那一段（分区 hash + 分片合并），
  属于 PR-7 以后的范畴。

### -race 验收

- `AppendRefPath` 在 `-race` 下干净（覆盖 PR-2/5 新代码的并发读写路径）。
- `litehead` 全量单测 `-race` 干净（含 `TestLabelCatalogConcurrentPutAndGet`）。

---

## 后续建议（未落地）

按照规划，后面可以考虑的方向有三条：

1. **sealed chunk per-window registry（规划 §5.1 第二步）**
   目标：**彻底删除** forced flush，用 per-flush-window 的 sealed chunk 索引
   替代 per-series 本地数组。需要先重构 blockReader 的 chunk 组装逻辑，
   影响面较大。建议在**运维侧确认 PR-4 的 forced flush Counter 长期为 0**
   之后再评估是否启动。

2. **labelCatalog 分代回收（规划 §5.2 第二步）**
   目标：解决 append-only 长期膨胀。需要引入 `flush window → arena
   generation` 的映射，以及 flush 完成后安全回收旧 generation chunks 的
   协议（包括 reader 在途的引用计数）。当前 `labelCatalogIncludesOversizedInSize`
   已把 arena 字节量暴露给监控，先用指标判断有没有膨胀问题。

3. **snapshot 合并阶段并行化（PR-5 遗留）**
   当前 loadSnapshot 合并阶段串行调 `createSeriesWithRef`，是 50k+ 场景下
   并行加速不到 10% 的主因。可以设计：按 `ref % N` 分片到 N 个 shard，
   每个 shard 有独立 refTab 分片与 hashIdx 分片；合并时 shard 内并行。
   但这会引入分片粒度的锁管理，权衡后留给后续专项。

---

## 验收矩阵（截至本轮）

- `go build ./...`：**干净**
- `go test ./tsdb/litehead/ -count=1 -timeout=180s`：**PASS** (1.5s)
- `go test -race ./tsdb/litehead/ -count=1 -timeout=300s`：**PASS** (6.2s)
- `go vet ./tsdb/litehead/`：**干净**
- `read_lints`：**无新增诊断**

一句话总结：**litehead 从"可用原型"推进到"工程可控"已完成；下一步方向由
生产数据与监控信号驱动决定，不再做无依据的结构改造。**
