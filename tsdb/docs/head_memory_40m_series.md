## Prometheus TSDB Head 内存分析：4000 万 Series 时哪里最占内存

这份文档专门回答一个问题：**如果 `Head` 里同时维护 4000 万条 series，主要是 `Head` 的哪几部分在吃内存？**

本文不讨论查询侧缓存、WAL 文件大小、磁盘 block 大小，而是聚焦在 `Head` 这块活跃内存区本身。

本文主要基于这些代码位置：

- `tsdb/head.go`
- `tsdb/head_append.go`
- `tsdb/head_read.go`
- `tsdb/ooo_head_read.go`
- `tsdb/index/postings.go`
- `model/labels/labels_stringlabels.go`
- `model/labels/labels.go`
- `tsdb/isolation.go`
- `.promu.yml`
- `web/api/v1/api.go`

---

## 先给结论

如果只从 `Head` 的顶层字段视角看，**4000 万 series 时内存大头通常就是这两块：**

- `Head.series`
- `Head.postings`

但是，真正占掉大部分内存的，并不是这两个字段本身，而是它们下面挂出去的大量对象：

- `stripeSeries` 的两套索引结构
- 每条 series 对应的 `memSeries` 常驻对象
- 每条 series 持有的 `lset`
- postings 里的 `[]SeriesRef`
- 当前仍在内存里可写的 `headChunks.chunk`

一句话概括就是：

> **4000 万 series 时，Head 内存通常先被“每条 series 的常驻元数据 + 标签 + 倒排索引”打高；如果这些 series 还都很活跃，再叠加“当前 chunk 的样本数据”继续往上冲。**

---

## 从 `Head` 顶层视角看：先盯住两块

对这份问题，最有价值的记法不是死记很多小字段，而是先抓住 `Head` 里两大块：

- **`Head.series`**：所有活跃 series 的总目录和主体对象都在这里
- **`Head.postings`**：按标签倒排组织出来的索引结构在这里

所以，如果你问：

> “4000 万 series 时，主要是 Head 的哪里占内存比较高？”

最短答案就是：

- **第一眼先看 `Head.series`**
- **第二眼看 `Head.postings`**

再往下拆，才能看到真正的大头细节。

---

## 先别误会：`Head.series` 和 `Head.postings` 不只是“查询用”

很多人第一次看到 `Head.series` 和 `Head.postings`，会直觉认为：

- `Head.series` 是为了查询时按 `ref` 找 series
- `Head.postings` 是为了查询时按标签倒排找 series

这个理解只对了一半。

更准确地说：

- **`Head.series` 是写入和查询都强依赖的主目录**
- **`Head.postings` 主要是查询索引，但写入在创建新 series 时也必须维护它**

### `Head.series` 的职责：不是“查询辅助”，而是 series 主表

在 `Head` 里，`series` 的类型是 `*stripeSeries`。它下面真正挂着的是所有活跃的 `*memSeries`。

你可以把它理解成：

- 按 `series ref` 找到对应的 `memSeries`
- 按 `labels hash` 找到对应的 `memSeries`
- 让写入路径知道“这条样本该落到哪条 series 上”
- 让查询路径知道“这个 `SeriesRef` 对应哪条真实 series”

也就是说，**`Head.series` 不是可有可无的查询缓存，而是 `Head` 里 series 主体对象的总目录。**

### 为什么写入强依赖 `Head.series`

在 `headAppender.Append()` 里，写入普通 float 样本时，第一步就会先做：

- `a.head.series.getByID(...)`

如果通过 `ref` 没找到对应 series，才会进一步走：

- `a.head.getOrCreate(...)`

而 `getOrCreate()` 内部第一步又会先做：

- `h.series.getByHash(hash, lset)`

只有 ref 找不到、hash + labelset 也找不到时，才会真正创建新的 `memSeries`。

这意味着，**写入一个样本之前，系统必须先靠 `Head.series` 定位“这条样本属于哪条 series”。**

如果没有 `Head.series`：

- 不知道这个样本该追加到哪个 `memSeries`
- 也不知道这是老 series 还是新 series
- 更没法继续把样本写进当前 head chunk

### 样本真正存放的位置也不是 `postings`

这里很容易误解。

样本不是写进 `Head.postings`，也不是写进 `Head.series` 这个字段本身。

真实关系更接近这样：

```text
Head.series
  -> memSeries
    -> headChunks
      -> chunk
```

也就是说：

- `Head.series` 负责把写入流量导向正确的 `memSeries`
- 样本真正追加的位置，是 `memSeries.headChunks.chunk`
- `Head.postings` 并不存样本值本身

### `Head.postings` 的职责：主要是倒排索引，但写入生命周期必须维护

`Head.postings` 的实际类型是 `*index.MemPostings`。

它本质上是一个倒排结构，大致可以理解成：

```text
label name
  -> label value
    -> []SeriesRef
```

它最核心的作用当然是查询：

- 给定标签条件，例如 `job="api"`
- 快速找出命中的 `SeriesRef`

所以从“主职责”来说，**`Head.postings` 确实更偏向查询侧索引。**

但是，它又不是“只查询时才需要”。

因为在 `Head.getOrCreateWithID()` 中，只要真的创建出一条新 series，就会立刻做：

- `h.postings.Add(storage.SeriesRef(id), lset)`

这一步非常关键，它表示：

- 新 series 不仅要进入 `Head.series`
- 还必须立刻进入 `Head.postings`

否则后续基于标签的查询、统计、筛选都找不到它。

### `Head.postings` 不只查询会用，GC 和统计也会用

除了创建时的 `Add()` 之外，`Head.postings` 还会在别的生命周期环节被使用：

- series 被 GC 删除后，会调用 `h.postings.Delete(...)`
- 统计基数时，会调用 `h.postings.Stats(...)`

所以更准确的说法是：

- **`Head.postings` 的主职责是标签倒排索引**
- **查询最依赖它**
- **但写入生命周期中的“新建 / 删除 / 统计维护”也依赖它**

### 查询时两者如何配合

查询路径里，这两个结构通常是串起来用的：

```text
label matcher
  -> Head.postings
  -> SeriesRef
  -> Head.series
  -> memSeries / chunks
```

也就是：

- 先通过 `Head.postings` 做标签筛选
- 拿到一批 `SeriesRef`
- 再通过 `Head.series.getByID(...)` 找到真正的 `memSeries`
- 最后再去读标签、chunk 元信息、样本数据

所以：

- **`Head.postings` 更像按标签反查的二级索引**
- **`Head.series` 更像 series 主目录 / 主表**

### 这和内存分析有什么关系？

这段职责分析之所以重要，是因为它能解释一个很常见的误区：

> 为什么 `Head.series` 和 `Head.postings` 会长期常驻内存，而且会在高基数场景下变成大头？

答案就是：

- **它们不是“查询来了再临时算一下”的结构**
- **它们是 Head 在正常运行过程中必须实时维护的常驻结构**

其中：

- `Head.series` 是写入和查询共用的 series 主目录
- `Head.postings` 是查询、统计以及生命周期维护要依赖的倒排索引

也正因为如此，到了 4000 万 series 规模时，这两块会稳定地成为内存分析里的重点对象。

---

## 第一大头通常是 `Head.series`

### 为什么 `Head.series` 会这么贵？

因为它不是一个简单的 map，而是一整套围绕 series 的常驻体系。

在 `tsdb/head.go` 里，`Head.series` 是 `*stripeSeries`。而 `stripeSeries` 会把同一条 series 同时挂到两套索引里：

- 一套按 `series ref` 查找
- 一套按 `labels hash` 查找

也就是说，**每条 series 至少会被两套索引同时引用。**

除此之外，每条 series 还会有自己的 `memSeries` 对象，其中包含：

- `ref`
- `lset`
- `mmappedChunks`
- `headChunks`
- `app`
- `lastValue`
- `pendingCommit`
- `txs`
- 各种时间边界、hash、histogram 状态字段

所以，`Head.series` 真正贵的地方不是某一行代码，而是：

- **每条 series 都有一份常驻对象**
- **每条 series 同时挂在两套索引里**
- **每条 series 还会继续挂标签、chunk、事务状态等附属对象**

### 一个很实用的理解：这是“每条 series 的固定税”

当 series 数从几万、几十万上涨到几千万时，`Head.series` 的可怕之处在于：

- 它不是按样本数增长
- 它是按 **series 条数** 线性增长

所以 4000 万 series 这种场景里，**哪怕每条 series 样本并不多，单靠“每条 series 的常驻骨架”也已经非常贵。**

### 可以怎么粗估？

按 64 位机器、当前代码布局做一个粗粒度理解：

- `memSeries` 本体：大约是 `200B` 级别
- 默认开启隔离时的 `txRing`：大约是 `70B` 级别
- 当前 `memChunk` 元数据：大约是 `40B` 级别

这只是帮助建立量级感，不是精确测量值。

如果先只看这些“骨架”，不算：

- `lset` 的实际字节
- map bucket 开销
- postings
- chunk 里的真实样本数据

那么每条 series 也已经接近 `300B+` 级别。

4000 万条 series 乘上去，量级就会非常夸张。

所以从经验上说：

- **`Head.series` 往往是 4000 万 series 场景的第一大头**

---

## `Head.series` 里最贵的，不只是 `memSeries` 本体

很多人看到 `memSeries`，会以为内存主要花在这个 struct 上。其实不完全对。

更准确地说，`Head.series` 这坨内存通常由几层东西一起组成：

- **`stripeSeries.series[...]`**
  - 按 ref 建索引
- **`stripeSeries.hashes[...]`**
  - 按 label hash 建索引
- **`memSeries` 本体**
  - 每条 series 一份常驻对象
- **`memSeries.lset`**
  - 每条 series 的标签集合
- **`memSeries.txs`**
  - 默认隔离开着时，每条 series 还会带事务 ring
- **`memSeries.headChunks`**
  - 当前还在内存里可继续写的 chunk 元数据
- **`memSeries.headChunks.chunk`**
  - 当前 chunk 的真实样本编码数据

所以：

- **`Head.series` 是一个总账名词**
- **真正花钱的是它下面成千上万万个 per-series 子对象**

---

## 第二大头通常是 `Head.postings`

### `Head.postings` 是什么？

`Head.postings` 的实际类型是 `*index.MemPostings`，实现位于 `tsdb/index/postings.go`。

它本质上是：

- label name
  - label value
    - `[]SeriesRef`

也就是：

- 一个 label pair 对应一串 series 引用列表

### 为什么它会被 4000 万 series 放大？

因为一条新 series 进入 `Head` 时，不只是建一个 `memSeries` 就结束了，还会被加入 postings。

在 `MemPostings.Add()` 里，会发生两件事：

- 对当前 label set 里的**每一个 label**都加入 postings
- 另外再加入一次全量 postings，也就是 `allPostingsKey`

这意味着：

- 如果每条 series 平均有 `L` 个 label
- 那么每条 series 至少会在 postings 里贡献 `L + 1` 个引用

于是，只有 postings 里的引用数，就大致会变成：

- `4000 万 * (L + 1)`

如果平均每条 series 有 `10` 个 label，那么：

- 总 postings 引用条目数量大约是 `4000 万 * 11`

而每个 `SeriesRef` 本身就要占字节，更别说还有：

- `[]SeriesRef` 的扩容冗余
- 两层 map 的 bucket 开销
- 高基数 label value 带来的大量键

### 一个很容易忽视的点：全量 postings 自己就很大

除了每个真实 label pair 之外，还会有一份 “all postings”。

也就是说，光是：

- “当前 Head 里所有 series 的 ref 全量列表”

这一个结构，到了 4000 万 series 规模，本身就已经是很可观的一块内存。

### 所以什么时候 `Head.postings` 会特别夸张？

当下面几种情况出现时，`Head.postings` 会涨得非常快：

- **每条 series 的 label 数比较多**
- **某些 label 的 value 基数非常高**
- **存在很多一次性或高 churn 的 label value**
- **同一批高基数 label pair 挂了大量 series**

因此：

- **如果你的问题是“高基数为什么特别吃内存”，`Head.postings` 是一定要重点看的。**

---

## `memSeries.lset` 往往也是多 GB 级的大头

### 标签不是“顺带占一点”，而是长期常驻

在 `memSeries` 里，每条 series 都要持有自己的 `lset`。

这意味着：

- 只要 series 还活着
- 它的标签集合就要一直跟着它常驻内存

到 4000 万 series 时，这块通常绝对不能忽略。

### 当前仓库默认已经开启了 `stringlabels`

根据 `.promu.yml`，当前仓库默认构建带有：

- `stringlabels`

这会让 `labels.Labels` 使用更紧凑的表示方式。

对比两种实现：

- `model/labels/labels.go`
  - 传统方式，更接近 `[]Label`
- `model/labels/labels_stringlabels.go`
  - 更紧凑，使用单段扁平字符串表示 label 集合

这说明：

- **当前默认构建已经在尽量压缩标签内存了**
- 但即便如此，4000 万 series 时，`lset` 仍然很可能是多 GB 级别

### 为什么标签这么贵？

因为它是一个完完全全的 per-series 成本：

- series 越多，标签副本越多
- 标签越长，这块越大
- 唯一值越多，可共享空间越少

所以：

- **如果你的 series 带着很长的 label value，或者带很多低复用的唯一值，`lset` 会非常贵。**

---

## `headChunks.chunk` 会不会成为最大头？要看 series 是否活跃

到这里，需要区分两个概念：

- **基数税**：只要 series 存在就要付
- **活跃样本税**：series 不但存在，而且正在持续写，才会继续抬高

`Head.series`、`lset`、`postings` 更偏向前者。
而 `headChunks.chunk` 更偏向后者。

### 场景 1：高基数，但很多 series 不活跃或样本很少

例如：

- 4000 万 series 存在
- 但其中很多 series 只写了很少样本
- 或者 churn 很高，很多只是短暂出现

这时通常更贵的是：

- `Head.series`
- `memSeries.lset`
- `Head.postings`

也就是说，**元数据和索引先把内存抬高。**

### 场景 2：4000 万 series 都持续活跃写入

例如：

- 每条 series 都在持续被 scrape
- 当前 `Head` 窗口里，大量 series 都有正在写的 chunk

这时：

- `memSeries.headChunks.chunk`

就会快速变得非常大，因为你相当于在同时维护海量“当前打开的 chunk”。

所以要记住：

- **高基数场景先爆元数据和倒排**
- **高活跃场景再叠加当前 chunk 数据**

---

## 一个更贴近实际的排序

如果不极端化，按很多线上高基数场景的直觉顺序，可以先这样记：

- **第一层**：`Head.series` 里的 per-series 常驻对象
- **第二层**：`memSeries.lset` 标签内容
- **第三层**：`Head.postings` 倒排索引
- **第四层**：`memSeries.headChunks.chunk` 当前 chunk 数据

但这个排序并不是永远固定，会被业务特征改写：

- **label 更多**：`postings` 会更重
- **label 更长**：`lset` 会更重
- **series 更活跃**：`headChunks.chunk` 会更重

所以，更准确的说法应该是：

> **4000 万 series 的 Head 内存，下限由“per-series 常驻税”决定，上限再被标签规模和活跃样本量继续抬高。**

---

## 哪些部分通常不是主矛盾？

### 不是 `Head` 顶层 struct 本身

`Head` 顶层那几个字段本身并不大。
真正大的，是它们引用出去的海量对象。

### 不是 `StripeSize` 的固定开销

默认 `StripeSize` 是固定的，确实会预分配一定结构，但和 4000 万 series 带来的动态对象相比，通常不是主因。

### 不是 `headAppender.samples` 这类瞬时缓冲

这种写入批次级别的缓冲会占内存，但它更像瞬时工作集，不是 4000 万 series 长时间驻留时的主账单。

### 不是历史 `mmappedChunks` 的“当前 Go heap 主账单”

`mmappedChunks` 指向的是已经 mmap 的 chunk。它当然也会体现在进程内存里，但如果你讨论的是 Head 的主要 Go heap 压力，通常不应先把锅甩给它。

---

## 一张记忆图：从字段到真正的大头

```text
Head
├── series
│   ├── stripeSeries.series[...]       按 ref 的索引
│   ├── stripeSeries.hashes[...]       按 label hash 的索引
│   ├── memSeries                      每条 series 的常驻对象
│   ├── memSeries.lset                 每条 series 的标签集合
│   ├── memSeries.txs                  隔离相关事务 ring
│   ├── memSeries.headChunks           当前 chunk 元数据
│   └── memSeries.headChunks.chunk     当前 chunk 的真实样本数据
└── postings
    ├── labelName -> labelValue
    └── []SeriesRef                    每个 label pair 对应的一串 series 引用
```

如果你只记一件事，就记这张图。

---

## 线上怎么验证，不要只靠猜

如果你在排查线上实例，建议优先看 TSDB 状态接口和离线分析工具。

### TSDB 状态接口里重点看什么？

在 `web/api/v1/api.go` 里，可以看到这些统计项：

- `headStats.numSeries`
- `memoryInBytesByLabelName`
- `seriesCountByLabelValuePair`

其中最有用的两个通常是：

- **`memoryInBytesByLabelName`**
  - 看哪个 label name 对应的 value 总字节最夸张
- **`seriesCountByLabelValuePair`**
  - 看哪个 label pair 挂的 series 最多

### 离线分析可以看什么？

可以使用：

- `promtool tsdb analyze`

它更适合查：

- 高基数 label
- label pair cardinality
- churn
- compaction 效率

这类信息非常适合帮助你判断：

- 问题到底更偏向 `postings`
- 还是更偏向 `lset`
- 还是 series 数本身已经高到离谱

---

## 最后的结论压缩版

可以把整份分析压缩成四句话：

- **从 `Head` 顶层字段看，4000 万 series 时首先盯 `Head.series` 和 `Head.postings`。**
- **从对象层看，最先把内存抬高的通常不是样本值本身，而是 per-series 常驻对象、标签和倒排索引。**
- **如果这些 series 还都持续活跃写入，`memSeries.headChunks.chunk` 也会迅速变成大头。**
- **因此，4000 万 series 的 Head 内存问题，本质上是“series 常驻税 + label 税 + postings 税 + 活跃 chunk 税”的叠加。**

---

## 如果只想记一句话

**4000 万 series 时，Head 的主要内存压力通常不只是样本数据本身，而是先来自 `Head.series` 下面的 per-series 常驻对象与标签，再来自 `Head.postings` 倒排索引；如果这些 series 还持续活跃写入，当前 `headChunks.chunk` 会继续把内存推高。**
