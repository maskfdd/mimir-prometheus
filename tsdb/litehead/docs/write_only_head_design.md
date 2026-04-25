## LiteHead 设计文档

这个包实现了一个 `LiteHead`。它只负责写入、切 chunk、写 WAL、落 block，不提供查询。目标很明确：**尽量压低 Head 的常驻内存，同时尽可能与标准 `tsdb.Head` 的外部语义对齐，方便作为 mimir-ingester 的替换项。**

对照阅读的代码位置：

- `tsdb/litehead/db.go`、`tsdb/litehead/appender.go`
- `tsdb/litehead/series.go`、`tsdb/litehead/label_catalog.go`
- `tsdb/litehead/flush.go`、`tsdb/litehead/blockreader.go`
- `tsdb/litehead/replay.go`、`tsdb/litehead/metrics.go`
- 参考原型：`tsdb/head.go`、`tsdb/head_append.go`、`tsdb/chunks/head_chunks.go`、`tsdb/compact.go`

---

## 1. 背景与目标

现有 `Head` 之所以重，本质上是因为它同时服务写入和查询。LiteHead 的思路很直接：**把查询相关结构全部拿掉，只保留写入闭环必需的状态；其它 API（MinTime/MaxTime、Appender、Compact、Truncate、WAL checkpoint）尽量贴近标准 Head 的语义。**

LiteHead 的目标只有五条：

- **只保留写入必需的最小 per-series 状态**
- **热路径按 `ref` 定位，`labels/hash -> ref` 只走冷路径**
- **稳态不维护 `postings` 和其它查询索引**
- **内存里只保留当前 open chunk，sealed chunk 立即 spill 到 `ChunkDiskMapper`**
- **flush 时把 LiteHead 自身包装成 `BlockReader` 直接喂给 `compactor.Write`**

---

## 2. 关键前提：写入必须走 `ref-first`

这个方案要想真正省内存，也依赖调用方配合。**最理想的写法是先拿到 `ref`，后续反复复用这个 `ref`。**

`storage.Appender` 支持 `Append(ref, lset, t, v)` 和 `GetRef(lset, hash)`，因此调用方可以缓存 `ref` 或批量查 `ref`，后续直接按 `ref` 写。

这里把这点当成硬约束：

> **热路径必须是 `ref-first`；`labels/hash -> ref` 只应存在于冷路径。**

如果调用方总是传 `ref=0`，存储层就必须做 labels hash 并走全局查找，CPU 和内存收益都会明显变差。

调用方需要做的事很简单：

- 缓存 `Append()` 返回的 `ref`
- 或在批量写入前先调用 `GetRef()`

---

## 3. 总体结构

整体结构并不复杂。**热路径靠 `refTab` 找 series，冷路径靠 `hashIdx` 找 series，labels 单独放在 `labelCat`；sealed chunk 通过 `ChunkDiskMapper` 落盘，WAL 记录写入元数据与样本，达到窗口阈值后由 `compactor.Write` 固化成 block。**

```
┌──────────────────────────────────────────────────────┐
│                         DB                            │
│                                                       │
│  ┌──────────┐   ┌──────────┐                          │
│  │  refTab  │   │  hashIdx │                          │
│  │(分页数组) │   │(分片 map) │                          │
│  └────┬─────┘   └────┬─────┘                          │
│       │              │                                │
│       └────► *memSeries ◄─┘                           │
│                │ labelsID                             │
│                ▼                                      │
│         ┌──────────────┐                              │
│         │  labelCat    │  arena + symbolTable         │
│         └──────────────┘                              │
│                                                       │
│  ChunkDiskMapper ◄── sealed chunk 字节                │
│  WAL (wlog)     ◄── RefSeries / RefSample             │
│  后台 goroutine ── 周期触发 compactHead / WAL truncate │
└──────────────────────────────────────────────────────┘
```

三个核心结构：

- `refTab`：分页数组，热路径 `ref -> *memSeries`
- `hashIdx`：分片 map，冷路径 `hash -> (ref, labelsID)`
- `labelCat`：labels 外移存储，`memSeries` 只持 `labelsID`，再叠一层 `symbolTable` 做字符串去重

外部依赖只有三块：`ChunkDiskMapper`、WAL、`tsdb.LeveledCompactor`。目录结构与原生 `tsdb.Head` 保持一致：

```
<dir>/
  wal/              WAL 段；Series / Samples / Checkpoint
  chunks_head/      ChunkDiskMapper 的 head chunk 文件
  <ULID>/           compactor.Write 生成的 block 目录
```

---

## 4. 核心数据结构

这部分只有一个原则：**热对象尽量瘦，冷信息尽量外移，命名尽量贴标准 Head。**

### 4.1 `refTab`：ref 分页数组

`refTab` 是热路径主索引。**因为 `ref` 是递增整数（从 1 开始），这里直接用分页数组，不用 `map`。**

```go
const (
    refPageShift = 14                 // 每页 16384 条
    refPageSize  = 1 << refPageShift
    refPageMask  = refPageSize - 1
)

type refTable struct {
    mu    sync.RWMutex
    pages []*refPage
}

type refPage struct {
    entries [refPageSize]*memSeries
}
```

关键点：

- `get(ref)` 先算页号，页外直接返回 `nil`
- `set` 按需追加 `nil` 槽页，再分配具体 `refPage`
- `del(ref)` 仅把槽位置空，ref 空间不回收（单调递增）
- `forEach` 在持有读锁的状态下遍历所有活跃 series，回调里不允许再取写锁

`len()` 是近似值，仅用于 metrics。

### 4.2 `hashIdx`：分片冷路径 map

`hashIdx` 只服务冷路径。**职责很简单：在没有 `ref` 时，帮你从 labels/hash 找回 series。**

```go
const hashStripeCount = 256

type hashIndex struct {
    locks   [hashStripeCount]sync.RWMutex
    buckets [hashStripeCount]map[uint64][]refEntry
}

type refEntry struct {
    ref      chunks.HeadSeriesRef
    labelsID uint32
}
```

发生 hash 冲突时，通过 `labelsID` 回读 labels 做 `labelCat.equals()` 校验，槽内不直接存 labels。

### 4.3 `labelCat`：labels arena + symbolTable

labels 不直接挂在 `memSeries` 上，而是统一放到 `labelCat`。**每条 series 只保留一个 `labelsID`（uint32）；更进一步，所有 label name/value 字符串都经过 `symbolTable` 去重。**

```go
type labelCatalog struct {
    mu    sync.RWMutex
    arena []byte     // 扁平编码：[uvarint(n) (uvarint(nameID) uvarint(valueID))*n]
    index []uint32   // labelsID -> arena offset
    syms  symbolTable
}

type symbolTable struct {
    mu   sync.RWMutex
    list []string          // symbolID -> string
    idx  map[string]uint32 // string -> symbolID
}
```

设计要点：

- arena 和 symbolTable 都是 append-only，单条不做 GC；回收只能在 arena 重建时做（留待后续）
- `put()` 先把 name/value 登记进 `symbolTable`，再以 uvarint(symbolID) 对的形式写入 arena
- `equals()` 走解码-比较路径，避免在 hash 冲突时反复构造 `labels.Labels`
- `get()` 会把字节拷贝一份再解码，规避 arena 并发 append 迁移底层切片带来的悬挂引用
- `symbolTable.intern()` 采用 `RLock → 双重检查 → WLock` 模式，把热路径锁成本压到最低

在时序场景里这层去重很关键：label name 集合通常只有几十个、value 也大量重复（job / instance / namespace 等），40M series 下可以显著压低 labels 的常驻内存。

### 4.4 `memSeries`：最小写入状态

`memSeries` 只保留"继续写下一条样本"必须用到的字段。**事务、查询、OOO 这些都不要；命名与标准 `tsdb.memSeries` 对齐，便于对照阅读。**

```go
type memSeries struct {
    mu sync.Mutex

    ref      chunks.HeadSeriesRef
    labelsID uint32

    // 最近一条已提交样本的时间戳，用来判定乱序。
    lastTs int64

    // open chunk 按需懒分配；无样本时为 nil。
    openChunk chunkenc.Chunk
    openApp   chunkenc.Appender
    openMinT  int64
    openMaxT  int64
    // 按 chunkRange 对齐的下一次切分边界。
    nextAt int64

    // 窗口内已经 spill 到磁盘的 sealed chunk 元数据。
    mmappedChunksCount uint8
    mmappedChunks      [maxMmappedChunksPerSeries]mmappedChunk
}

// 单条 series 在 flush 之前可持有的 sealed chunk 上限。
// 稳态下通常只有 1~2 个；超过上限走 forced flush。
const maxMmappedChunksPerSeries = 8

type mmappedChunk struct {
    ref        chunks.ChunkDiskMapperRef
    minTime    int64
    maxTime    int64
    encoding   chunkenc.Encoding
    numSamples uint16 // 仅估算/监控用
}
```

这里**不引入** `txRing`、`pendingCommit`、`mmappedChunks 链表`、OOO 状态等字段。

---

## 5. 写入路径

写入路径的核心也很简单：**有 `ref` 就直达，没有 `ref` 才走 hashIdx 冷路径；写入过程中只维护最小状态，WAL 在 `Commit` 时批量落盘。**

### 5.1 `Append`

流程对齐标准 Head 的 in-order 分支：

1. `resolveSeries`：
   - `ref != 0` 时先查 `refTab`；命中直接返回
   - 未命中或 ref 不存在，按 labels 做 `HasDuplicateLabelNames`、`WithoutEmpty`，再查 `hashIdx`
   - 仍未命中：`db.createSeries()` 分配 ref、写 labelCatalog、注册两张索引，并把 `RefSeries` 加到 `pendingSeries`
2. `t < db.appendableMinValidTime()` 直接返回 `ErrOutOfBounds`
3. 取 `s.mu`，校验乱序：
   - 跨 batch：`s.lastTs != MinInt64 && t <= s.lastTs` → `ErrOutOfOrderSample`
   - batch 内：`s.openChunk` 非空时 `t <= s.openMaxT` → `ErrOutOfOrderSample`
4. `ensureOpenChunk`：open chunk 懒分配
5. `maybeCutChunk`：按大小 / `nextAt` / 样本数 / 编码 判定是否切 chunk；需要时先 `sealAndSpillLocked` 再 `cutNewChunkLocked`
6. `openApp.Append(t, v)`、更新 `openMaxT`
7. 把样本加到 `pendingSamples` / `sampleSeries`

热路径只经过 `refTab.get` 和 `memSeries.mu`，不维护 postings，也不碰全局查询结构。

### 5.2 `Commit` / `Rollback`

`Commit` 分三步：

1. `logWAL()`：一次性写 `RefSeries`（新建 series）与 `RefSample`（本批样本）
2. 在 WAL 落盘后再更新每条 series 的 `lastTs` 与全局 `minTime/maxTime`——顺序颠倒会在崩溃时出现"内存已推进、WAL 没样本"的悖论
3. 维护 metrics：`samplesAppended` / `outOfOrderSamples`

`Rollback` 会丢弃本批样本，但仍然写入 `pendingSeries`：如果不写，后续的 sample 会引用一个 WAL 中不存在的 ref，WAL replay 时就会丢失样本。

`appender` 对象用 `sync.Pool` 复用；`reset()` 负责清空切片并放回 pool。

### 5.3 chunk 切分与 spill

**内存里只保留当前正在写的 chunk；一旦封口，就立刻写到 `ChunkDiskMapper`。**

切分触发条件沿用现有 Head：

- 初次写（`openChunk == nil`）
- 时间到达 `nextAt`
- 样本数超过 `2 * SamplesPerChunk`
- XOR 字节数超 `chunkenc.MaxBytesPerXORChunk - 19`
- 编码发生变化

`sealAndSpillLocked` 做四件事：

1. 取出当前 `openChunk`，若为空 / 无样本，直接丢弃
2. 通过 `chunkDiskMapper.WriteChunk(ref, mint, maxt, chunk, false, nil)` 异步写
3. 把 `(chunkRef, minTime, maxTime, encoding, numSamples)` 追加到 `mmappedChunks[]`
4. 清空 `openChunk/openApp`，等待下一条样本懒分配

**`mmappedChunks[]` 满怎么办**（`maxMmappedChunksPerSeries = 8`）：这是一个特殊路径，绝不允许丢数据。

- 临时释放 `s.mu`
- 在释放前先把当前已知的 `openMaxT` 与所有 `mmappedChunks.maxTime` 推进到 `db.maxTime`，否则后续 flush 选择窗口时会看到一个过小的上界
- 调 `db.flushBlocking()` 把当前所有样本同步 flush 成 block
- 重新拿 `s.mu`，做一次保底扫描：`mmappedChunks[i].maxTime > flushedMaxt` 才保留
- `db.flushBlocking` 里的 `compactHeadWindowOpts(gcSeries=false)` 只清 `mmappedChunks` / CDM，**不**回收 series，避免 appender 手里还握着 `*memSeries` 时被 UAF
- 计数器 `prometheus_tsdb_litehead_mmapped_chunks_forced_flush_total` 打点

---

## 6. Flush / Compact

**LiteHead 稳态不维护查询形态的结构；到 flush 时才把一个时间窗口临时包装成 `BlockReader` 喂给 compactor。**

### 6.1 触发与窗口选择

后台 goroutine `db.run()` 以 `FlushCheckInterval` 周期 tick，命中 `shouldFlush` 后调用 `compactHead`：

```go
// 与标准 Head.compactable() 一致：MaxT - MinT > 1.5 * ChunkRange 时压窗。
maxt-mint > (ChunkRange*3)/2
```

每次 `compactHead` 切出左侧一个 `BlockDuration` 宽度的窗口 `[mint, rangeForTimestamp(mint, BlockDuration)-1]`，命名对齐 `tsdb.DB.compactHead`。

### 6.2 两类 flush 入口

| 方法 | 场景 | `gcSeries` | 备注 |
|------|------|-----------|------|
| `compactHead()` | 后台周期 | `true` | 完整 GC：truncate → sweepDeadSeries |
| `flushBlocking()` | appender 触发的 forced flush | `false` | appender 还握着 `*memSeries`，不删 series |
| `tryFlushAll()` | `Close()` | 最后一个窗口 `true` | 其余窗口 `false`，保证和 flushBlocking 一致 |

三者都走 `compactHeadWindowOpts(mint, maxt, gcSeries)`，只是 GC 策略不同。

### 6.3 `compactHeadWindowOpts` 关键步骤

1. **推进 `minValidTime` 到 `maxt+1`**：防止样本在 flush 期间掉进已 flush 区间（对齐标准 Head）
2. **构造 `liteHeadBlockReader`**（见 §6.4）：若窗口内无数据，直接走 truncate 路径并结束
3. **`NewLeveledCompactor` + `compactor.Write(dir, br, mint, maxt+1, nil)`**——区间是半开的 `[mint, maxt+1)`，与原生 Head flush 约定一致
4. **失败回滚 `minValidTime`**：保证失败后 Append 仍然可以写原窗口的数据
5. **成功后 truncate**：
   - `gcSeries=true` → `truncateMemory(maxt)`：`advanceMinTime` + `truncateMmapped` + `sweepDeadSeries`
   - `gcSeries=false` → `truncateMemoryKeepSeries(maxt)`：只做前两步
6. **`truncateWAL(maxt)`**：用 `wlog.Checkpoint` 落新段，`keep` 函数沿用"ref 仍在 `refTab`"作为保留条件；checkpoint 文件清理 + `walTruncateDuration` 打点

`truncateMmapped` 会：

- 压缩每条 series 的 `mmappedChunks[]`，丢弃 `maxTime <= flushMaxt` 的条目
- 如果 `openChunk` 的 `openMaxT <= flushMaxt` 则清空（它的样本已经进 block）
- 收集仍被引用的最小文件号，调用 `chunkDiskMapper.Truncate()` 释放旧文件段
- 更新 `labelCatalog*` 相关 gauge

`sweepDeadSeries` 回收的前提是：`openChunk == nil && mmappedChunksCount == 0 && lastTs <= flushMaxt`。labelCatalog 里 `labelsID` 仍然驻留（append-only 限制）。

### 6.4 `liteHeadBlockReader`：BlockReader 快照

这是一个**只读、一次性**的视图，生命周期仅在 `compactor.Write` 调用期间。

构造：

- 在 `refTab.forEach` 中快照每条 series：过滤不在窗口内的，收集 `mmappedChunks`（属于窗口的条目）以及 `openChunk`（若命中窗口）
- **open chunk 的字节在快照阶段就 `copy` 冻结**，避免 flush 期间 appender 继续 `Append` 导致字节迁移
- 按 `labels.Compare` 排序，`symbolSet` 去重排序

暴露的接口：

- `Index()` → `liteHeadIndexReader`：
  - `Postings` 只可靠支持 `AllPostings`（compactor 实际会调的形态）
  - `SortedPostings` 基于快照重排
  - `ShardedPostings` 用 `labels.StableHash` 过滤
  - `Series(ref, builder, chks)` 按快照返回 labels 和 `chunks.Meta`（open chunk 的 `MaxTime = MaxInt64`，对齐原生"可增长"语义）
  - 其它 `LabelNames/LabelValues/PostingsForMatchers` 为防御式实现，compactor 不会走到
- `Chunks()` → `liteHeadChunkReader`：按 `HeadChunkRef` 拆出 series+chunk id，`mmapped` 走 `ChunkDiskMapper.Chunk()`，`open` 用 `chunkenc.FromData` 解码冻结字节
- `Tombstones()` 固定返回空
- `Meta()` 只给出 `MinTime/MaxTime`；真正落盘的 block ULID 由 `compactor.Write` 自己生成

**这个路径相比"临时 Head + WAL 回放"的优势**：省掉了样本"解码 → 重 append → 再编码"的一轮拷贝，flush 期间的堆分配显著降低。

### 6.5 `appendableMinValidTime`

对齐标准 Head：返回 `minValidTime` 与"当前可能正在被 compact 的 compaction window 起点"中的较大值：

```go
cwEnd := maxt - ChunkRange/2
return max(minValidTime, cwEnd)
```

这两重保护分别对应两类丢数据风险：

- `minValidTime`：样本落到已 flush 的窗口之前
- `cwEnd`：样本落进当前 flush 还没完成的窗口

`setMinValidTime` 用 CAS 实现单调递增。

---

## 7. WAL replay

重启恢复时只重建映射关系和最后时间戳，**不重建 open chunk**。

`Open` 时依次做：

1. **`replayChunkDiskMapper`**：遍历所有 head chunk，拿到最大的 `HeadSeriesRef`，用来恢复 `nextRef`（后续 WAL replay 还会继续更新）
2. **`replayWAL`**：先加载 checkpoint，再逐段读 WAL 段
   - `record.Series` → `createSeriesWithRef`：按 WAL 中的 ref 重建 `refTab / hashIdx / labelCat`，同一 ref 重复出现保留先出现的那份
   - `record.Samples` → 按 ref 查 `refTab`，找不到直接跳过（series 可能已被 GC），找到则更新 `lastTs` 与全局 min/max
   - `Tombstones / Exemplars / Metadata / HistogramSamples / FloatHistogramSamples / MmapMarkers / 未知类型` 一律忽略：LiteHead 不使用

**为什么不恢复 open chunk 也不丢数据？** 因为任何已 `Commit` 的样本都在 WAL 里，随后的周期 flush 会从 WAL 读到它们（via `liteHeadBlockReader` 构造时的 refTab 快照），落进 block；`lastTs` 的恢复足以阻挡 replay 之后的乱序样本。

失败时 `wlog.Repair` 兜底一次，失败再返回聚合错误。

---

## 8. 代码组织

落在 `tsdb/litehead/` 单独包内，**不改造现有 `Head`**。

| 文件 | 内容 | 现状行数 |
|------|------|---------|
| `db.go` | `DB` 结构、`Open/Close`、`createSeries`、`appendableMinValidTime`、`updateMinMaxTime` | ~420 |
| `appender.go` | `storage.Appender` 实现、chunk 切分与 spill、forced flush | ~400 |
| `series.go` | `memSeries`、`refTable`、`hashIndex`、`mmappedChunk` | ~235 |
| `label_catalog.go` | `labelCatalog` + `symbolTable` | ~250 |
| `flush.go` | 周期 flush / 强制 flush / Close flush、truncateMemory、truncateWAL | ~390 |
| `blockreader.go` | `liteHeadBlockReader / IndexReader / ChunkReader` | ~440 |
| `replay.go` | ChunkDiskMapper 回放 + WAL replay | ~190 |
| `metrics.go` | 对齐标准 Head 的指标 | ~180 |

补充：

- 通过独立包切换，调用方选择性注入 `litehead.DB` 代替 `tsdb.Head`
- 尽量复用现有能力：`storage.Appender` 接口、WAL 记录格式、`ChunkDiskMapper`、`chunkenc`、`tsdb.LeveledCompactor`、block 目录格式

---

## 9. 优化前后内存占用对比

在 **4000 万 series** 的目标场景下，Head 常驻内存预计可以从 **~39 GB** 降到 **~9.8 GB**，降幅约 **75%**。

以下估算只看 Head 稳态 heap，不含 flush 瞬时峰值；假设平均 10 个 label、大部分写入复用 `ref`、label name/value 有较高重复率。

| 内存构成 | 现有 Head | LiteHead | 说明 |
|---------|----------|---------------|------|
| `postings`（倒排索引 + 全量 postings） | ~10 GB | **0** | 完全不维护 |
| series 主索引（`stripeSeries` 双 map） | ~6 GB | **~1.5 GB** | 分页数组替代 map；`hashIdx` 只存 `(ref, labelsID)` |
| labels 本体 | ~6 GB | **~2.3 GB** | arena + symbol table 去重，`memSeries` 只留 `labelsID` |
| series 对象本身 | ~12 GB | **~5 GB** | `memSeries` 字段数从 ~25 降到 ~12 |
| head chunks（含 mmappedChunks 链） | ~5 GB | **~1 GB** | 只保留 openChunk；sealed 立即 spill；最多 8 个 mmappedChunks 元数据 |
| **合计** | **~39 GB** | **~9.8 GB** | **降幅 ~75%** |

收益主要来自六处：

- **去掉 `postings`**：write-only 不做标签查询，整块移除
- **压缩主索引**：`map[ref]*memSeries` → 分页数组；`hashIdx` 只存 `(ref, labelsID)`
- **外移 labels**：labels 统一放到 arena
- **label 字符串去重**：`symbolTable` 把 40M series 下的重复 name/value 合并
- **瘦身 `memSeries`**：去掉事务、查询、OOO 字段
- **及时 spill sealed chunk**：内存只保留 openChunk；sealed 以定长 8 槽元数据存在

收益会被削弱的场景：

- **调用方不复用 `ref`**：冷路径热化，`hashIdx` / `labelCat.equals` 访问变多
- **所有 series 持续活跃**：每条都长期占用 openChunk
- **label 非常长或非常多**：symbol table + arena 都会变大
- **series churn 很高**：labelCat arena 无法复用旧条目（append-only）

---

## 10. 与标准 Head 的语义对齐点

写这一节是因为 LiteHead 想能直接作为 ingester 里 `tsdb.Head` 的替换项，使用方的 Prometheus 面板 / 告警 / 关机流程都不应该感知变化：

- **目录布局**：`wal/` + `chunks_head/` + `<ULID>/` 一致
- **Append 语义**：in-order 样本；乱序 → `storage.ErrOutOfOrderSample`；越界 → `storage.ErrOutOfBounds`
- **`appendableMinValidTime`**：包含 `minValidTime` + compaction window 保护
- **`MinTime/MaxTime`** 命名与返回值一致（空态返回 `MinInt64`）
- **chunk 切分触发条件**：`nextAt` / `2x SamplesPerChunk` / XOR 字节上限 / 编码变化
- **compactHead 窗口选择**：`MaxT - MinT > 1.5 * ChunkRange`
- **truncate 流程**：`truncateMemory` + `truncateWAL`（checkpoint 再删旧段）
- **metrics**：`prometheus_tsdb_head_*`、`prometheus_tsdb_compactions_*`、`prometheus_tsdb_wal_truncate_duration_seconds`、`prometheus_tsdb_checkpoint_*`、`prometheus_tsdb_data_replay_duration_seconds` 均命名一致，面板/告警不用改

LiteHead 有意不提供的：

- Query / ChunkQuery / ExemplarQuery：`Querier` 直接返回 `ErrQuerierUnsupported`
- Exemplar / Histogram / Metadata 写入：`Append*` 现为 no-op 占位
- WBL、Tombstones、OOO 样本

---

## 11. 风险与权衡

这个方案不复杂，但有几个前提不能忽略：

- **调用方 `ref` 复用率**决定收益上限
- **flush 成本上移**：稳态不维护查询结构，flush 时要一次性快照 series / open chunk，窗口内 series 越多越贵
- **forced flush 代价不低**：单条 series 的 `mmappedChunks[]` 满时会同步触发 `flushBlocking`，如果一段时间内有大量 series 触顶会叠加成抖动；`prometheus_tsdb_litehead_mmapped_chunks_forced_flush_total` 是关键观测
- **labelCatalog 不做单条回收**：arena + symbolTable 都是 append-only；churn 高的租户需要等 arena 重建（后续版本补齐）
- **WAL replay**：不恢复 open chunk，依赖下一轮 flush 从 WAL 流式把样本写成 block——这条语义和标准 Head 不完全一致，使用方需要了解

---

## 12. 实施范围

本包只做写入闭环，不做 Head 查询。**凡是需要查询的需求，都应该去读 flush 后的 block。**

本包包含：

- in-order float 样本写入
- `refTab` / `hashIdx` / `labelCat`（含 `symbolTable`）
- chunk 切分 + sealed spill 到 `ChunkDiskMapper`（forced flush 兜底）
- 周期 / 强制 / Close 三类 flush，统一走 `liteHeadBlockReader` + `LeveledCompactor.Write`
- WAL + replay（不重建 openChunk）
- 对齐标准 Head 的监控指标 + litehead 特有的少量指标

验收标准：

- 能持续 ingest
- 能 WAL replay
- 能 flush 出合法 block
- 稳态内存显著低于标准 Head

**架构红线：Head 查询、`postings`、label 查询都不在范围内。** 如果为了查询再把索引加回去，这个方案的内存收益模型就失效了。

---

## 13. 术语说明

- **`ref`**：series 在 Head 内部的整数 ID。调用方缓存它后，后续写入可以直接定位到目标 series。
- **`ref-first`**：优先用 `ref` 写入，而不是每次都根据 labels 做 hash 和查找。
- **稳态 / steady-state**：系统进入平稳运行后的常态。这里主要指非启动、非 replay、非 flush 瞬时峰值时的常驻内存状态。
- **`postings`**：为标签查询服务的倒排索引。LiteHead 不维护这部分。
- **open chunk**：当前仍在追加样本的 chunk。
- **sealed chunk / mmappedChunk**：已经切分完成、通过 `ChunkDiskMapper` 落到磁盘的 chunk。
- **spill**：把 sealed chunk 的字节从内存写到 `ChunkDiskMapper`，只在内存里留下最小元数据。
- **flush / compact**：把一个时间窗口内的数据整理成 block 并写到磁盘。
- **forced flush**：单条 series 的 `mmappedChunks[]` 满时触发的同步全库 flush。
- **WAL**：Write-Ahead Log。样本和 series 先写 WAL，再视情况 flush 成 block。
- **WAL replay**：进程重启后重放 WAL，恢复内存状态。
- **arena**：一块连续的内存区域。这里用于紧凑存储 labels，减少零散对象和指针开销。
- **symbolTable**：append-only 字符串池，给每个不同的 label name / value 分配一个 uint32 ID。
- **`BlockReader`**：`compactor.Write` 所需的读取视图。这里由 `liteHeadBlockReader` 实现，只在 flush 期间临时存在。
- **OOO**：Out-Of-Order，指时间戳乱序写入。本方案只处理 in-order 写入。
