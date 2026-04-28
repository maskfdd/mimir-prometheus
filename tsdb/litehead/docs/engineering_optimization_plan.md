# litehead 工程优化方案（Engineering Review）

> 基于当前 `litehead` 代码逻辑与 `optimization_notes.md`，输出一份**可执行的工程优化方案**。
> 
> 目标不是再做一版高层判断，而是回答：**先改什么、具体改哪里、怎么拆阶段、如何验收、有哪些风险要控。**

---

## 1. 范围与目标

### 1.1 目标

本方案只面向 `litehead` 的目标场景：**write-only ingester**。

优化目标按优先级排序：

1. **先补正确性闭环**：crash / replay / snapshot / flush 语义可验证
2. **再拿高 ROI 优化**：常驻内存、flush CPU、flush 峰值内存
3. **最后做规模化稳定性**：降低 forced flush 尾延迟，控制长期膨胀，缩短重启时间

### 1.2 非目标

以下方向不在本轮方案内：

- 不补查询链路，不回加 `postings`
- 不追求与标准 `Head` 完全等价的运行态恢复
- 不优先做 lock-free / 自旋锁 / 大规模锁模型重写
- 不优先做 open chunk 零拷贝

---

## 2. 当前实现摘要（按代码路径）

### 2.1 写入路径

当前写入主链路在：

- `appender.go`
- `series.go`
- `head.go`

核心特点：

- `Append()` 走 `ref -> refTab` 快路径，`labels -> hashIdx` 走冷路径
- `memSeries` 保存最小写入状态：`lastTs`、`openChunk/openApp`、`mmappedChunks`
- `sealAndSpillLocked()` 把 sealed chunk 写入 `ChunkDiskMapper`
- `Commit()` 先写 WAL，再更新 `lastTs` 和全局 `min/max time`

当前主要问题：

- `memSeries.mmappedChunks [8]` 固定成本过高
- `AppendExemplar` / `AppendHistogram` / `UpdateMetadata` 静默返回成功
- 单条 series 超过 `maxMmappedChunksPerSeries` 时会触发 `flushBlocking()`，把局部压力放大成全局停顿

### 2.2 flush / blockReader 路径

当前 flush 主链路在：

- `flush.go`
- `blockreader.go`

核心特点：

- `compactHeadWindowOpts()` 先推进 `minValidTime`，再构造 `newBlockReader()`
- `newBlockReader()` 会遍历 `refTab`，锁每条 `memSeries`，收集 window 内的 chunk 描述
- open chunk 在快照阶段通过 `make + copy` 冻结字节
- `symbolSet` 目前通过遍历全部 series 再 decode labels 构造

当前主要问题：

- `newBlockReader()` 全表遍历 + per-series 加锁，flush 抖动较大
- open chunk copy 会制造较高瞬时内存峰值
- `symbolSet` 收集路径重复 decode，CPU 不划算

### 2.3 replay / snapshot 路径

当前恢复主链路在：

- `replay.go`
- `snapshot.go`

核心特点：

- snapshot 和 replay 只恢复 `refTab/hashIdx/labelCatalog` + `lastTs` + `min/max time`
- 不恢复 open chunk 运行态
- 当前已有 snapshot / replay 相关测试，但覆盖面仍偏“功能可用”，缺少失败/重试/边界测试

当前主要问题：

- correctness 证据还不够体系化
- snapshot / replay 仍是单线程，后续会成为大库启动瓶颈

### 2.4 labels 路径

当前 labels 相关逻辑在：

- `label_catalog.go`

核心特点：

- `labelCatalog` 使用 append-only `arena []byte + index []uint32`
- `symbolTable` 做字符串去重
- `sliceLocked()` 每次都会 `make + copy`

当前主要问题：

- `get/compare/equals` 都要复制一份字节
- arena append-only，长期 churn 下只增不减

---

## 3. 分阶段优化方案

## Phase 0：正确性与语义补强（上线前置项）

### 目标

把“代码逻辑上看起来成立”变成“测试证明成立”。

### 具体改动

#### P0-1. unsupported 写入类型显式失败

**改动点**：`appender.go`

将以下方法从 `return 0, nil` 改为显式返回错误：

- `AppendExemplar()`
- `AppendHistogram()`
- `UpdateMetadata()`

建议新增：

```go
var ErrUnsupportedWriteType = errors.New("litehead: exemplar/histogram/metadata writes are not supported")
```

**原因**：避免静默丢数据。

**影响面**：仅 `litehead` 内部和接入方错误处理逻辑，不影响 float sample 正常写入路径。

#### P0-2. 补 crash / replay / snapshot / flush 测试矩阵

**新增测试文件建议**：

- `snapshot_test.go`：继续扩充 snapshot + 增量 WAL 组合场景
- `flush_test.go`：新增 flush 失败与重试语义测试
- `appender_test.go`：新增 unsupported 写入测试

**测试矩阵至少覆盖**：

- `Commit` 成功、flush 前 crash
- snapshot 成功、checkpoint/WAL truncate 前 crash
- flush 失败后重试
- forced flush 与正常 flush 交错
- 重启后继续写，再 flush 出 block
- unsupported 写入返回明确错误

#### P0-3. 文档明确语义边界

**文档更新点**：

- `optimization_notes.md`
- `replacement_analysis.md`

明确说明：

- `litehead` 是 write-only head
- `Meta()` 当前是内部兼容语义，不是外部稳定身份
- 当前仅支持 in-order float samples

### 验收标准

- 新增测试全部通过
- 至少覆盖 1 条 crash → reopen → flush → verify block 的完整链路
- 不支持写入类型不再静默成功

---

## Phase 1：高 ROI 优化（优先做）

## 4.1 优化一：`mmappedChunks` 瘦身

### 设计

将 `memSeries` 从固定：

- `mmappedChunksCount uint8`
- `mmappedChunks [8]mmappedChunk`

改为：

- `sealedCount uint8`
- `sealedInline mmappedChunk`
- `sealedOverflow []mmappedChunk`

并新增辅助方法，避免业务代码到处判断结构分支：

- `sealedLen()`
- `sealedAt(i int) *mmappedChunk`
- `appendSealed(mc mmappedChunk)`
- `retainSealedAfter(maxt int64)`
- `forEachSealed(fn func(mmappedChunk))`

### 影响文件

- `series.go`
- `appender.go`
- `blockreader.go`
- `flush.go`

### 原则

- 先保持外部语义不变，只替换内部表示
- 不在第一步改动 forced flush 策略，先单独拿内存收益

### 收益

- 显著降低 `memSeries` 固定体积
- 对 Append hot path 风险最小

### 风险控制

- 先保留 `maxMmappedChunksPerSeries` 语义
- 先通过结构替换 + 测试等价验证，再考虑下一阶段是否继续弱化 forced flush

### 验收

- 原有 `head_test.go` 中的 flush / overflow / GC 用例全部通过
- 新增结构回归测试：`sealedLen/retainSealedAfter` 边界正确

---

## 4.2 优化二：flush 时 `symbolSet` 直接取 `symbolTable`

### 设计

当前 `newBlockReader()` 为了构造 `symbolSet`，会遍历全部 series 再通过 `labelCat.get()` decode labels。

建议改为：

- 在 `symbolTable` 上新增只读快照方法，例如 `snapshotList()`
- `newBlockReader()` 直接基于 `symbolTable` 构造 `symbolSet`

示意：

```go
func (s *symbolTable) snapshotList() []string
```

### 影响文件

- `label_catalog.go`
- `blockreader.go`

### 原则

- 允许 `symbolSet` 是超集
- 不追求“只包含活跃 window 内 symbols”这种昂贵精确性

### 收益

- 直接消除一轮全量 labels decode
- flush CPU 和瞬时内存都会下降

### 风险控制

- block symbols table 可能略大，但这是可以接受的
- 只要 compactor 能正常消费 symbols 超集，就不影响正确性

### 验收

- block 生成结果保持可读、可 reopen
- 新增测试：有已删除 series / 多 window 情况下，block 仍合法

---

## 4.3 优化三：open chunk snapshot 改为 pooled scratch

### 设计

当前 `newBlockReader()` 在快照 open chunk 时直接：

- `make([]byte, len(b))`
- `copy(frozen, b)`

目标不是一步做到零拷贝，而是先把“每次 flush 都产生大量短命大对象”的问题改掉。

建议方案：

1. 在 `Head` 增加专用 `snapshotBufPool`
2. `newBlockReader()` 从 pool 获取 buffer，复制 open chunk 字节
3. 在 reader 生命周期结束时归还 buffer

### 关键工程问题

`blockReader` 本身没有 `Close()`，因此不能简单“借了 pool 就不还”。建议配套做一个小型生命周期管理：

- `blockReader` 增加 `releaseOnce sync.Once`
- `IndexReader.Close()` 和 `ChunkReader.Close()` 都回调到 `blockReader.release()`
- `blockReader` 内部维护 refcount，两个 reader 都关闭后统一归还 buffer

### 影响文件

- `head.go`
- `blockreader.go`

### 原则

- 先做 pooled copy，不做零拷贝
- 生命周期要比性能更优先

### 收益

- 显著降低 flush 峰值内存和 GC 压力
- 风险远低于零拷贝

### 风险控制

- 如果 compactor 生命周期与预期不一致，先退回“reader 私有大 slab，不回 pool”的方案，确保正确性优先

### 验收

- 原有 flush / reopen 测试通过
- 新增 benchmark：flush 期间分配次数和峰值对象数下降

---

## Phase 2：规模化稳定性优化

## 5.1 优化四：forced flush 降级为“两步走”方案

### 背景

当前 `sealAndSpillLocked()` 在 `mmappedChunks` 满时直接调用 `flushBlocking()`。这保证了正确性，但会把单条热点 series 的局部问题放大成全局停顿。

### 方案

#### 第一步：短期降风险，不改核心语义

在 Phase 1 完成后，先做两件事：

- `mmappedChunks` 改成 inline + overflow 后，允许 overflow slice 在合理范围内增长
- 将 forced flush 从“频繁触发”降到“极端保护”

也就是说，**短期先把 forced flush 变成罕见兜底，而不是完全删掉。**

#### 第二步：中期重构 sealed chunk 跟踪模型

如果后续仍要彻底去掉 per-series → 全局 forced flush，需要先解决一个核心问题：

> 当前 block flush 依赖 per-series 持有 `mmappedChunk ref` 才能把 sealed chunk 重新组装成 block。

因此不能简单“把最老的 sealed ref 丢掉”；否则 chunk 还在磁盘上，但 blockReader 已经找不到它，等价于数据丢失。

中期可选方案：

- 设计**按 flush window 组织的 sealed chunk registry**
- 或设计更轻量的 per-window flush 索引，而不是只靠 `memSeries` 本地数组

### 结论

- **本轮不建议直接删 forced flush**
- 正确路线是：**先把它变成极少触发的兜底，再重构跟踪模型**

### 影响文件

- `appender.go`
- `series.go`
- `blockreader.go`
- `flush.go`

---

## 5.2 优化五：`labelCatalog` 演进为稳定 arena

### 设计

当前 `sliceLocked()` 每次 `make + copy`，导致：

- `get/compare/equals` 都有额外分配
- 高 churn 场景下 arena 只增不减

建议分两步：

#### 第一步：两级 arena

将：

- `arena []byte`

演进为：

- `chunks [][]byte`
- `chunkSize` 固定
- `index` 记录 `(chunkID, offset)` 或全局 offset

目标是让 `sliceLocked()` 可以返回稳定切片，而不是每次复制。

#### 第二步：分代回收

按 flush window 或代际维护 label arena，降低高 churn 下的单调膨胀风险。

### 影响文件

- `label_catalog.go`
- `flush.go`（如果引入分代回收）

### 原则

- 不做 stop-the-world rebuild
- 优先稳定结构和减少 alloc，再谈长期回收

### 验收

- `hashIdx.get()` 与 `labelCat.compare()` 语义不变
- 大批量写入 benchmark 下 alloc/op 明显下降

---

## 5.3 优化六：snapshot / replay 并行化

### 设计

当前 `writeSnapshot()` 与 `loadSnapshot()` 本质都是全表扫描/顺序读写。

建议：

- snapshot：按 `refPage` 并行编码，串行 `cp.Log`
- replay：按 segment 或 record batch 并行 decode，再串行合并到主结构

### 影响文件

- `snapshot.go`
- `replay.go`

### 原则

- 优先做可控并行：编码/解码并行，状态合并串行
- 避免直接并发修改 `refTab/hashIdx/labelCat`

### 验收

- snapshot / replay 时间下降
- 不引入 ref 冲突、重复 series 或 lastTs 回退

---

## 4. 实施顺序与拆 PR 建议

### PR-1：正确性与语义

- `ErrUnsupportedWriteType`
- unsupported 写入测试
- crash / replay / flush 测试矩阵补齐
- 文档补边界

### PR-2：`mmappedChunks` 结构瘦身

- 替换固定数组为 inline + overflow
- 补辅助方法与结构测试
- 复跑所有 flush / GC / overflow 用例

### PR-3：flush 低风险优化

- `symbolSet` 直接取 `symbolTable`
- open chunk pooled scratch
- benchmark / pprof 对比

### PR-4：规模化稳定性第一步

- 弱化 forced flush 触发频率
- 明确保底上限和监控
- 压测观察 Append P99

### PR-5：中期结构演进

- `labelCatalog` 两级 arena
- snapshot / replay 并行化
- 视结果决定是否继续推进 sealed chunk registry

---

## 5. 验收指标

### 功能正确性

- 所有新增 crash / replay / flush 测试通过
- block 可被 `tsdb.OpenBlock()` 打开
- block 统计信息（series / samples）符合预期

### 性能指标

建议至少补 3 组 benchmark：

- `BenchmarkLiteHeadAppendRefPath`
- `BenchmarkLiteHeadFlushBlockReader`
- `BenchmarkLiteHeadSnapshotWriteAndLoad`

建议关注：

- `alloc/op`
- `B/op`
- flush duration
- replay duration
- Append P99

### 监控指标

压测期重点看：

- `mmapped_chunks_forced_flush_total`
- flush duration
- snapshot/replay duration
- series active / removed
- labelCatalog size / symbols size

---

## 6. 最终建议

如果只看最近两到三轮迭代，我建议按下面顺序推进：

1. **先补正确性闭环和 unsupported 显式失败**
2. **再做 `mmappedChunks` 瘦身**
3. **再做 flush 的 `symbolSet` 和 pooled snapshot buffer**
4. **最后再处理 forced flush、labelCatalog、并行 replay 这些更深层改造**

核心原因很简单：

- 前三项**收益大、边界清晰、风险可控**
- forced flush 与 labelCatalog 重构虽然重要，但都更容易牵动语义和长期维护成本

一句话总结：**先把 `litehead` 从“原型可用”推进到“工程可控”，再把它从“工程可控”推进到“规模化可放量”。**
