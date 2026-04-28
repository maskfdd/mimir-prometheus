# litehead 优化建议（CEO Review）

> 目标：给出 `litehead` 是否值得继续做、影响面在哪里、最该优先做哪些优化。

---

## 结论

**值得继续做。**

`litehead` 适合的是 **write-only ingester**，不是通用 `Head` 替代。如果目标场景是顺序写、查询靠 block，那么这条路线是对的，核心价值在于：

- **显著降低 Head 常驻内存**
- **降低 flush 路径的资源开销**
- **保留现有 TSDB/block 生态，外部接入成本低**

但当前更像是**可用原型 / 早期工程化阶段**。下一步最重要的不是继续扩优化列表，而是先把最关键的正确性和高 ROI 优化做扎实。

---

## 影响面

### 1. 代码影响面

优化主要集中在 `litehead` 内部，对外接入方式影响相对有限。

重点会落在这些模块：

- `series.go`
- `appender.go`
- `blockreader.go`
- `flush.go`
- `replay.go`
- `snapshot.go`

也就是说，**主要是内部实现优化，不是大范围改外部调用协议**。

### 2. 运行时影响面

最直接的影响有四类：

- **内存**：`memSeries` 体积、`mmappedChunks` 固定开销、flush 峰值内存
- **CPU**：flush 阶段 labels decode、snapshot/replay 成本
- **延迟**：per-series 压力触发全局 forced flush，会放大 Append P99
- **启动恢复**：snapshot/WAL replay 的完整性和恢复速度

### 3. 产品语义影响面

这个项目必须一直守住一个边界：**`litehead` 是 write-only head**。

因此需要明确：

- 不要把它当成查询型 `Head`
- 不支持的写入类型不能静默成功
- 内部兼容语义不能被外部误当成稳定协议

---

## 当前最需要关注的风险

### 1. 正确性闭环

当前最需要先确认的是：

- `Commit` 后 crash、flush 前重启
- snapshot 后 crash
- flush 失败后重试
- 重启后继续写再 flush

这些场景必须通过测试矩阵证明正确。**这是第一优先级。**

### 2. 静默丢 unsupported 数据

如果 `AppendExemplar`、`AppendHistogram`、`UpdateMetadata` 继续静默返回成功，外部系统会误以为写入成功，实际上数据丢了。这个风险很高，而且完全没必要保留。

### 3. forced flush 放大尾延迟

单条热点 series 的局部压力，不应该把全局 flush 一起拉起来。这是后续最值得处理的稳定性问题之一。

---

## 最好的优化意见

### 1. `mmappedChunks` 改成 `1 inline + overflow`

这是**最值得优先做的常驻内存优化**。

原因很简单：

- 当前固定数组对每条 series 的成本太高
- 稳态下大多数 series 并不需要那么多 sealed chunk 槽位
- 改动集中，收益大，对 hot path 风险低

**结论：这是第一优先级的结构优化。**

### 2. flush 时 `symbolSet` 直接取 `symbolTable`

这是**最典型的小改动高收益优化**。

收益点：

- 减少 flush 阶段全量 labels decode
- 降低 flush CPU
- 降低 flush 瞬时内存

**结论：代码量小，收益直接，应该尽快做。**

### 3. open chunk snapshot 先改成 `buffer pool`

这是**最稳妥的 flush 峰值内存优化**。

建议不要一开始就追零拷贝，而是先：

- 用 pool 复用 buffer
- 先把 flush 峰值内存打下来
- 等语义和生命周期完全打实后，再考虑零拷贝

**结论：先拿 80% 收益，风险最低。**

### 4. unsupported 能力显式返回 `ErrUnsupported`

这是**最值得立刻做的产品安全优化**。

优点：

- 改动很小
- 可以立刻消除“静默丢数据”风险
- 能让外部接入方更快暴露错误用法

**结论：应视为上线前置项。**

### 5. 第二阶段处理 per-series 触发全局 forced flush

这是**最值得做的尾延迟优化**，但建议放在第一波稳妥优化之后。

因为它的收益确实很高，但影响链路更深：

- 涉及 flush 策略
- 涉及 chunk 生命周期
- 涉及更完整的正确性验证

**结论：高价值，但放在第二阶段更稳。**

---

## 推荐顺序

### 第一阶段：先补稳

- crash/replay/snapshot 正确性矩阵
- unsupported 能力显式失败
- 文档补清楚 write-only 边界

### 第二阶段：先拿高 ROI

- `mmappedChunks` 瘦身
- `symbolSet` 直接取 `symbolTable`
- open chunk snapshot `buffer pool`

### 第三阶段：做规模化稳定性

- 去掉 per-series → 全局 forced flush
- `labelCatalog` 结构演进
- replay / snapshot 并行化

---

## 一句话总结

**如果只选最值得做的三件事：先补 correctness，其次做 `mmappedChunks` 瘦身，再做 flush 路径上的 `symbolSet` 和 open chunk buffer pool 优化。**
