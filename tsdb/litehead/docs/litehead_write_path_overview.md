## LiteHead 指标写入链路梳理

这份文档聚焦 `tsdb/litehead` 的**创建、写入、落盘、恢复**四条主线，回答下面几个问题：

- `LiteHead` 是怎么创建出来的？
- 指标到底是写到哪个"环境"里？
- 一条样本写入时做了哪些操作？
- 数据分别在**内存**、**WAL**、**`chunks_head`**、**block**里保存什么？
- 当前实现已经支持什么，哪些能力还没有真正接通？

为了避免误解，先给一个最重要的结论。

---

## 1. 先说结论

### 1.1 当前仓库里的真实状态

**当前仓库里，`litehead` 还是一个独立包，还没有接入 Prometheus 生产写入路径。**

也就是说，在当前代码状态下：

- `scrape`
- `rules`
- `remote write`
- `OTLP write receiver`

这些入口**默认不会把样本写进 `litehead`**；它们仍然沿着现有链路写入 `tsdb.DB` / `tsdb.Head`。

目前仓库里能直接看到 `litehead.NewHead(...)` 的地方主要是测试，例如 `tsdb/litehead/head_test.go`。

### 1.2 如果单独实例化 `LiteHead`

如果调用方显式执行：

- `litehead.NewHead(logger, reg, dir, opts)` + `h.Init()`

那么写入会进入一个**独立的数据目录 `dir`**，目录结构如下：

```text
<dir>/
  wal/              WAL 段和 checkpoint
  chunks_head/      sealed chunk 的 head chunk 文件
  <ULID>/           flush 之后生成的标准 TSDB block 目录
```

### 1.3 一条 float 样本的主路径

当前 `litehead` 真正打通的主路径，是**普通 float、按时间递增（in-order）样本**：

```text
调用方拿到 Appender
  -> Append(ref, labels, timestamp, value)
  -> 找到或创建 series
  -> 写入当前 open chunk（内存）
  -> 记录 pendingSamples（本次批次）
  -> Commit()
  -> 先写 WAL
  -> 再更新 lastTs / Head 的 minTime/maxTime
  -> 必要时 seal 当前 chunk 并 spill 到 chunks_head/
  -> 外部调用方按需调用 Flush() compact 成 block
```

因此，`LiteHead` 不是"只在内存里存一下"的结构，而是一个**写优化的 Head**：

- 热数据先进入内存中的 open chunk
- 已 seal 的 chunk 很快写入 `chunks_head/`
- 所有已提交样本都会进入 `wal/`
- 周期性 flush 后再生成标准 TSDB block

---

## 2. `LiteHead` 是如何创建的

### 2.1 入口函数

创建入口在 `tsdb/litehead/head.go`：

- `func NewHead(logger log.Logger, reg prometheus.Registerer, dir string, opts *Options) (*Head, error)`
- `func (h *Head) Init() error`

`NewHead(...)` 创建 Head 实例并打开底层 WAL + ChunkDiskMapper；`Init()` 负责回放 WAL 恢复状态。对齐标准 `tsdb.Head` 的 `NewHead` + `Init` 模式。

### 2.2 初始化步骤

创建顺序可以概括为两阶段：

**`NewHead` 阶段（构造）：**

1. **校验并补齐 `Options`**
   - 包括 `ChunkRange`、`BlockDuration`、`SamplesPerChunk`、`WALSegmentSize` 等。
2. **创建目录锁**
   - 避免同一个目录被多个 `LiteHead` 实例同时打开。
3. **打开 WAL**
   - 在 `<dir>/wal` 下创建或打开 WAL。
4. **打开 `ChunkDiskMapper`**
   - 在 `<dir>/chunks_head` 下管理 sealed chunk 文件，并通过 mmap 读取。
5. **初始化内存索引和对象池**
   - `nextRef`
   - `refTab`
   - `hashIdx`
   - `labelCat`
   - `seriesPool`
   - `appenderPool`

**`Init` 阶段（WAL 回放）：**

6. **回放 `ChunkDiskMapper`**
   - 主要为了重新打开 mmap 文件，并恢复最大的 series ref。
7. **回放 checkpoint + WAL**
   - 恢复 `ref -> series`、`lastTs`、`MinTime/MaxTime`、`nextRef`。

### 2.3 初始化后的内存结构

`Head` 里最重要的几个字段是：

- `wal`
  - 已提交样本的 WAL 持久化入口。
- `chunkDiskMapper`
  - 管理 `chunks_head/` 里的 sealed chunk 文件。
- `refTab`
  - 热路径主索引，`ref -> *memSeries`。
- `hashIdx`
  - 冷路径索引，`labels hash -> ref`。
- `labelCat`
  - labels 外置存储，减少每条 series 的常驻内存。
- `minTime/maxTime`
  - 当前 Head 中样本时间范围。
- `minValidTime`
  - 防止 flush/truncate 窗口内旧样本被重新写回。

---

## 3. 指标写入到哪个"环境"

这个问题要分成**当前仓库状态**和**`litehead` 自身语义**两层来看。

### 3.1 当前仓库状态：生产流量并没有进入 `litehead`

当前主程序写入路径大致是：

```text
scrape / rules / remote write / OTLP
  -> storage.Appendable
  -> readyStorage
  -> fanoutStorage
  -> tsdb.DB
  -> tsdb.Head
```

所以**现在默认写入的"环境"是标准 `tsdb.DB` / `tsdb.Head`**，不是 `litehead`。

也就是说，仓库中虽然已经有 `tsdb/litehead` 包，但它还没有在 `cmd/prometheus/main.go` 里被注入到 `localStorage` 或 `fanoutStorage` 后面。

### 3.2 如果未来接入 `litehead`

如果未来把本地存储替换成 `LiteHead` 风格的实现，那么上游入口理论上都可以复用同一套 `storage.Appendable` 接口：

- scrape manager
- rule manager
- remote write receiver
- OTLP write receiver

它们最终只要拿到的是 `litehead.Head.Appender(...)`，就会沿 `LiteHead` 的写链路走下去。

### 3.3 `litehead` 自己的数据环境

如果单独使用 `LiteHead`，那么它的数据环境是一个**本地目录型持久化环境**，不是远端对象存储，也不是只靠进程内内存：

- **内存**：保存活跃 series 的最小状态和 open chunk
- **`wal/`**：保存已提交批次的 WAL 记录
- **`chunks_head/`**：保存 sealed chunk 的二进制文件
- **`<ULID>/` block**：保存 flush 之后的标准 TSDB block

所以它本质上仍是**本地 TSDB 写入层**，只是比标准 `Head` 更偏"write-only"和"低常驻内存"。

---

## 4. 谁会调用 `LiteHead` 的写接口

### 4.1 直接入口：`Head.Appender(...)`

`LiteHead` 对外暴露的是标准 `storage.Appender` 入口：

- `func (h *Head) Appender(_ context.Context) storage.Appender`

每次调用会从 `appenderPool` 里取出一个批量写入器 `appender`。

### 4.2 `appender` 是本次批量写入的事务对象

`litehead` 的写入不是"一条样本一个系统调用"，而是走批处理模型：

- 一个 `Appender` 生命周期里可以连续调用多次 `Append(...)`
- 最后调用一次 `Commit()` 或 `Rollback()`

`appender` 内部维护三组暂存：

- `pendingSeries`
  - 本批次新建的 series
- `pendingSamples`
  - 本批次追加的样本
- `sampleSeries`
  - 与 `pendingSamples` 对应的 `*memSeries` 指针

这些暂存使得 `Commit()` 可以一次性写 WAL，并在 WAL 成功后统一更新 `lastTs` 与全局时间窗。

---

## 5. 一条样本的详细写入链路

下面只看当前已经真正实现的主路径：**float sample**。

### 5.1 `Append(ref, lset, t, v)` 进入 `LiteHead`

调用 `Appender.Append(...)` 时，会先进入 `resolveSeries(ref, lset)`。

这里分两种情况：

#### 情况 A：调用方已经有 `ref`

这是热路径：

- 如果 `ref != 0`
- 就直接走 `refTab.get(ref)`
- 命中后拿到对应的 `*memSeries`

这是 `litehead` 设计里最希望发生的路径，因为它最省 CPU、最少依赖 labels 比较。

#### 情况 B：没有 `ref`

这是冷路径：

1. 清理空 label
2. 检查重复 label name
3. 计算 `lset.Hash()`
4. 在 `hashIdx` 里查找是否已有对应 series
5. 如果找不到，则创建新 series

### 5.2 创建新 series 时做了什么

如果是新 series，会调用 `Head.createSeries(hash, lset)`，主要做 4 件事：

1. **把 labels 放进 `labelCat`**
   - 返回一个 `labelsID`
   - `memSeries` 自己不直接常驻保存整套 labels
2. **分配新的 `ref`**
   - `nextRef` 单调递增
3. **构造 `memSeries`**
   - 初始 `lastTs = math.MinInt64`
4. **注册到两个索引**
   - `refTab.set(ref, s)`
   - `hashIdx.put(hash, ref, labelsID)`

然后这条新 series 会进入当前批次的 `pendingSeries`，用于 `Commit()` 时写 WAL。

### 5.3 时间合法性检查

在真正写样本前，会检查：

- `t < appendableMinValidTime()`

如果命中，直接返回 `storage.ErrOutOfBounds`。

这个检查的意义是：

- 一旦某个时间窗口已经准备 flush / truncate
- 后续样本不能再写回这个窗口之前
- 防止数据漏进已经要被 compact 掉的窗口

### 5.4 乱序检查

拿到 `memSeries` 后，会在 `s.mu` 保护下检查乱序：

- 如果 `t <= s.lastTs`
- 或者当前 batch 已经写到了 open chunk，且 `t <= s.openMaxT`

则返回 `storage.ErrOutOfOrderSample`。

因此，`LiteHead` 当前语义是：

- **只接受 in-order 样本**
- 不支持 OOO（out-of-order）写入路径

### 5.5 open chunk 的创建

如果当前 series 还没有 open chunk，会懒分配：

- `ensureOpenChunk(...)`
- 内部调用 `cutNewChunkLocked(...)`

这个过程会：

1. 创建一个空 chunk（当前主路径通常是 XOR chunk）
2. 创建 chunk appender
3. 初始化：
   - `openChunk`
   - `openApp`
   - `openMinT`
   - `openMaxT`
   - `nextAt`

其中 `nextAt` 表示当前 chunk 的时间边界，达到该边界时需要切 chunk。

### 5.6 判断是否需要切 chunk

每条样本在写入前都会经过 `maybeCutChunk(...)`。

触发切 chunk 的条件主要有 4 类：

1. **chunk 太大**
   - XOR chunk 字节数接近上限
2. **时间跨过 `nextAt`**
   - 达到 `ChunkRange` 的时间边界
3. **样本数过多**
   - `>= 2 * SamplesPerChunk`
4. **编码变化**
   - 当前实现里基本还是围绕 XOR float 路径

如果需要切 chunk，就会先把旧的 open chunk seal 掉，再创建新的 open chunk。

### 5.7 seal 旧 chunk，并 spill 到磁盘

`sealAndSpillLocked(...)` 做的事情很关键。

如果当前 open chunk 非空：

1. 计算 `mint` / `maxt`
2. 调用 `chunkDiskMapper.WriteChunk(...)`
3. 把返回的 chunk 引用记录到 `s.mmappedChunks[]`
4. 清空：
   - `s.openChunk`
   - `s.openApp`

这意味着：

- **chunk 的真正字节不再常驻内存中的 `memSeries`**
- 只留下一个很小的 `mmappedChunk` 元数据
- chunk 本体已经进入 `chunks_head/`

这是 `litehead` 节省内存的关键点之一。

### 5.8 把样本写进当前 open chunk

如果不需要切 chunk，或者切完后已经拿到新 open chunk，就会执行：

- `s.openApp.Append(t, v)`

同时更新：

- `s.openMaxT`

然后样本会被加入本次批次的：

- `pendingSamples`
- `sampleSeries`

注意这里的含义：

- **样本值已经进入 open chunk 的内存编码状态里**
- 但 `lastTs` 和全局时间窗还没有最终提交
- WAL 也还没写

这就是 `Appender` 批处理模型的中间态。

---

## 6. `Commit()` 阶段到底做了什么

`Commit()` 是 `LiteHead` 写入链路里最关键的提交点。

### 6.1 先写 WAL

`Commit()` 第一件事是 `logWAL()`，把本批次的：

- `pendingSeries`
- `pendingSamples`

编码成 WAL 记录并写入 `wal/`。

当前实现只写两类记录：

- `record.Series`
- `record.Samples`

### 6.2 为什么必须先 WAL 再更新内存状态

代码里明确强调：

- 必须先保证 WAL 已经写成功
- 然后才能更新 `lastTs`

这样做是为了保证崩溃一致性：

- 如果进程在 WAL 成功前崩掉，`lastTs` 不能先前移
- 否则重启后会出现"WAL 里没有样本，但内存状态已经认为它写过了"的问题

### 6.3 WAL 成功后更新提交态

WAL 成功后，`Commit()` 会遍历 `sampleSeries` / `pendingSamples`：

1. 更新每条 series 的 `lastTs`
2. 更新全局 `Head.minTime` / `Head.maxTime`
3. 增加监控指标

此时这批样本才真正进入**已提交状态**。

---

## 7. `Rollback()` 做了什么

`Rollback()` 不会保留样本，但有一个细节很重要：

- 如果当前批次里新建了 series
- 这些 series 仍会通过 `logOnlyPendingSeries()` 写入 WAL

原因是：

- `ref` 已经分配出去了
- 如果 WAL 里完全没有对应的 `Series` 记录
- 后续恢复时就可能出现 ref 指向不存在 series 的问题

所以 `Rollback()` 的语义不是"完全当作什么都没发生"，而是：

- **样本回滚**
- **series 定义仍然要持久化**

---

## 8. 数据到底在哪：内存、WAL、磁盘、block 的边界

这个问题是理解 `LiteHead` 的关键。

### 8.1 常驻内存里有什么

稳态下，内存里主要保留：

- `refTab`
- `hashIdx`
- `labelCat`
- 每条 `memSeries` 的最小状态
- 当前还在写的 `openChunk`
- 少量 `mmappedChunks[]` 元数据

`memSeries` 里保存的核心字段包括：

- `ref`
- `labelsID`
- `lastTs`
- `openChunk`
- `openMinT/openMaxT`
- `nextAt`
- `mmappedChunks[]`

注意：

- `mmappedChunks[]` 只是**元数据引用**
- sealed chunk 的大块字节本体不常驻在 `memSeries` 中

### 8.2 WAL 里有什么

`wal/` 里保存的是**已提交事务的日志记录**。

当前 `litehead` 主路径里真正依赖的是：

- `Series` 记录
- `Samples` 记录

WAL 的作用不是直接服务查询，而是：

- 崩溃恢复
- 重建 `series/ref/lastTs`
- 在需要时为 flush 后的数据兜底

### 8.3 `chunks_head/` 里有什么

`chunks_head/` 保存的是**已经 seal 的 head chunk 字节**。

特点是：

- 由 `ChunkDiskMapper` 管理
- 文件会通过 mmap 打开
- `memSeries` 里只保留引用，不保留整块原始字节

可以把它理解成：

- open chunk 还在"写态内存"
- sealed chunk 进入"本地磁盘上的 head chunk 文件"

### 8.4 block 目录里有什么

当 flush 触发后，会生成标准 TSDB block，目录名是一个 ULID，内部是正常 block 结构，例如：

- `meta.json`
- `index`
- `tombstones`
- `chunks/`

这部分已经是标准 TSDB block 语义，后续查询应走 block，而不是直接查 `LiteHead`。

### 8.5 可以把四层存储理解成什么关系

```text
第 1 层：open chunk（内存，正在写）
第 2 层：WAL（提交日志）
第 3 层：chunks_head（已 seal 的 head chunk 文件）
第 4 层：TSDB block（flush 后的正式块）
```

它们不是互斥关系，而是不同生命周期阶段的存储形态。

---

## 9. Flush 链路

### 9.1 什么时候会触发 flush

LiteHead 是被动组件，不内置后台 goroutine。外部调用方通过 `IsCompactable()` 判断是否满足 flush 条件，满足时调用 `Flush()` 触发。`FlushCheckInterval` 是建议外部调用方按此周期调用 `Flush()` 的间隔。

触发条件与标准 Head 类似：

- `MaxTime - MinTime > 1.5 * ChunkRange`

也就是当当前 Head 的时间跨度足够大时，就把所有满足条件的窗口逐一切出来，生成 block。

### 9.2 flush 的主流程

`Flush()` 内部调用 `tryFlushAll()`，`compactHeadWindowOpts()` 的主步骤如下：

1. 计算本次窗口 `[mint, maxt]`
2. 推进 `minValidTime = maxt + 1`
   - 阻止新样本再写回这个窗口
3. 用 `newBlockReader(...)` 对当前 Head 做只读快照
4. 调用 `tsdb.LeveledCompactor.Write(...)`
   - 直接把 `LiteHead` 快照写成 block
5. compact 成功后执行：
   - `truncateMemory(...)`
   - `truncateWAL(maxt)`

### 9.3 `blockReader` 的作用

`LiteHead` 没有查询能力，但 flush 时需要把窗口内数据喂给 compactor。

这里的做法不是"重放到临时 Head 再 compact"，而是：

- 直接把 `LiteHead` 自己包装成一个 `tsdb.BlockReader`

这样做的好处是：

- 不需要把样本"解码 -> 再 append -> 再编码"一遍
- 可以直接从：
  - `mmappedChunks` 对应的 `ChunkDiskMapper`
  - 当前 open chunk 的冻结字节
  读取数据

### 9.4 flush 后的内存清理

flush 成功后会做两层清理：

1. **`truncateMmapped(maxt)`**
   - 删除窗口前的 `mmappedChunks[]`
   - 释放已完成 flush 的 open chunk
   - 让 `ChunkDiskMapper.Truncate(...)` 删除更旧的 `chunks_head` 文件
2. **`sweepDeadSeries(maxt)`**（在允许 GC 的路径上）
   - 对那些已经没有 open chunk、没有 mmapped chunk、且 `lastTs <= maxt` 的 series 做回收

所以 `LiteHead` 不是"series 永不清理"，而是会在 flush 后回收已经闲置的 series 索引项。

### 9.5 WAL 截断

`truncateWAL(maxt)` 会：

1. 先滚到下一个 WAL segment
2. 选取一段历史 segment 做 checkpoint
3. 只保留仍然在 `refTab` 里的 series
4. 只保留 `mint >= maxt` 之后还可能继续需要的数据
5. 截断旧 segment
6. 删除更旧的 checkpoint

所以 WAL 不是无限增长，而是会随着 flush / truncate 一起向前推进。

---

## 10. 重启恢复链路

### 10.1 启动时先回放 `chunks_head`（Init 阶段）

`replayChunkDiskMapper()` 的作用主要有两个：

1. 打开已有的 mmap head chunk 文件
2. 找到最大的 `HeadSeriesRef`，恢复 `nextRef` 下界

它**不会**完整重建每条 series 的 `sealed[]` 状态。

### 10.2 再回放 checkpoint + WAL（Init 阶段）

`replayWAL()` 会先找最新 checkpoint，再继续读取后续 WAL segment。

当前恢复时重点恢复的是：

- `ref -> series` 映射
- 每条 series 的 `lastTs`
- 全局 `minTime/maxTime`
- `nextRef`

### 10.3 有意不恢复什么

当前实现明确**有意不做**下面这些事情：

- 不重建 postings
- 不重建 open chunk
- 不回放 exemplars / metadata / tombstones / histogram samples
- 不把旧 `chunks_head` 里的 sealed chunk 重新完整挂回每条 series 的运行态结构

这背后的思路是：

- `LiteHead` 本来就不提供查询
- 已 `Commit` 的样本都已经在 WAL 里
- 后续 flush 可以继续依赖现有的 WAL / chunk 信息完成落 block
- `lastTs` 的恢复足够保障写入顺序语义

---

## 11. 当前支持范围与限制

### 11.1 已经真正支持的能力

当前实现已经打通的主链路是：

- 普通 float sample
- in-order append
- 新建 series
- WAL 写入
- chunk seal + spill 到 `chunks_head`
- 周期 flush 成 block
- checkpoint + WAL replay
- Close 时尽量 flush 剩余数据

### 11.2 当前仍是占位或 no-op 的能力

下面这些接口目前主要是"为了兼容接口而存在"：

- `AppendHistogram(...)`
- `AppendExemplar(...)`
- `UpdateMetadata(...)`

当前实现里它们基本是**吸收式 no-op**：

- 不报错
- 不真正持久化数据
- replay 时也直接忽略对应 WAL 类型

因此要特别注意：

**如果把当前版本的 `litehead` 直接接到 Prometheus 通用入口上，float sample 主路径能工作，但 histogram / exemplar / metadata 语义并没有完整实现。**

### 11.3 查询能力

`LiteHead` 不提供：

- `Querier`
- `ChunkQuerier`
- `ExemplarQuerier`

这些接口会直接返回：

- `ErrQuerierUnsupported`

也就是说，它不是一个"可读写的完整 Head"，而是一个**偏写入、偏 flush、偏持久化的 write-only Head**。

---

## 12. 用一张图总结完整生命周期

```text
NewHead(dir)
  -> 打开 wal/
  -> 打开 chunks_head/
  -> 初始化 refTab/hashIdx/labelCat

Init()
  -> 回放 ChunkDiskMapper
  -> 回放 checkpoint + WAL

Appender.Append(...)
  -> resolveSeries
  -> 必要时 createSeries
  -> 检查 minValidTime / out-of-order
  -> 确保 open chunk
  -> 必要时 seal old chunk -> 写 chunks_head/
  -> 样本写入 open chunk（内存）
  -> 暂存到 pendingSamples

Commit()
  -> 先写 WAL（Series / Samples）
  -> 再更新 lastTs / minTime / maxTime

Flush()（由外部调用方驱动）
  -> 构造 blockReader 快照
  -> compactor.Write(...) 生成 block
  -> truncateMemory
  -> truncateWAL

Close()
  -> 尝试 flush 剩余窗口
  -> 做 checkpoint
  -> 关闭 chunkDiskMapper / WAL / lock
```

---

## 13. 最后的理解框架

如果只记住一句话，可以记这句：

**`LiteHead` 是一个"低常驻内存、以写入和 flush 为中心"的本地 TSDB Head：活跃样本先写内存 open chunk，提交后写 WAL，seal 后进 `chunks_head/`，最终再 compact 成标准 block。**

再补两句关键限定：

- **当前仓库里它还没有真正接入生产写入链路**
- **当前真正完成的是 float / in-order 主路径，histogram、exemplar、metadata 还不是完整实现**
- **LiteHead 是被动组件，不内置后台 goroutine；compaction/flush 由外部调用方按需驱动**

如果后续要继续深入，建议对照阅读：

- `tsdb/litehead/head.go`
- `tsdb/litehead/appender.go`
- `tsdb/litehead/series.go`
- `tsdb/litehead/flush.go`
- `tsdb/litehead/blockreader.go`
- `tsdb/litehead/replay.go`
