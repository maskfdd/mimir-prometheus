## Prometheus TSDB 轻量写入 Head 设计文档

这个方案引入一个 `WriteOnlyHead`。它只负责写入、切 chunk、写 WAL、落 block，不负责查询。目标很明确：**尽量压低 Head 的常驻内存。**

参考代码位置：

- `tsdb/head.go` / `tsdb/head_append.go`
- `tsdb/chunks/head_chunks.go`
- `tsdb/agent/db.go` / `tsdb/agent/series.go`
- `storage/interface.go`
- `tsdb/compact.go`

---

## 1. 背景与目标

现有 `Head` 之所以重，本质上是因为它同时服务写入和查询。这个方案的思路很直接：**把查询相关结构全部拿掉，只保留写入闭环必需的状态。**

当前 `Head` 会长期维护 `Head.series`、`Head.postings`、`memSeries`、`mmappedChunks`、`RangeHead` 等结构。这些结构对查询有价值，但对纯写入场景是额外负担。

`WriteOnlyHead` 的目标只有五条：

- **只保留写入必需的最小 per-series 状态**
- **热路径按 `ref` 定位，`labels/hash -> ref` 只走冷路径**
- **steady-state 不维护 `postings` 和其他查询索引**
- **内存里只保留当前 open chunk，sealed chunk 立即落盘**
- **flush 时临时整理成 block，复用现有 `compactor.Write()`**

---

## 2. 关键前提：写入必须走 `ref-first`

这个方案要想真正省内存，也依赖调用方配合。**最理想的写法是先拿到 `ref`，后续反复复用这个 `ref`。**

`storage.Appender` 已支持 `Append(ref, lset, t, v)` 和 `GetRef(lset, hash)`，因此调用方可以缓存 `ref`，后续直接写入。

本方案把这点当成硬约束：

> **热路径必须是 `ref-first`；`labels/hash -> ref` 只应存在于冷路径。**

如果调用方总是传 `ref=0`，存储层就必须对 labels 做 hash 并走全局查找，CPU 和内存收益都会明显变差。

调用方需要做的事很简单：

- 缓存 `Append()` 返回的 `ref`
- 或在批量写入前先调用 `GetRef()`

---

## 3. 总体结构

整体结构并不复杂。**热路径靠 `refTable` 找 series，冷路径靠 `hashIndex` 找 series，labels 单独放在 `labelCatalog`。**

```
┌─────────────────────────────────────────────────────┐
│                   WriteOnlyHead                     │
│                                                     │
│  ┌──────────────┐   ┌──────────────┐                │
│  │  refTable    │   │  hashIndex   │                │
│  │ (分页数组)    │   │ (分片 map)    │                │
│  └──────┬───────┘   └──────┬───────┘                │
│         │                  │                        │
│         └────► *writeSeries ◄───┘                   │
│                  │ labelsID                         │
│                  ▼                                  │
│            ┌──────────────┐                         │
│            │ labelCatalog │                         │
│            └──────────────┘                         │
│                                                     │
│  ChunkDiskMapper ◄── sealed chunk 字节              │
│  WAL ◄── RefSeries / RefSample                      │
└─────────────────────────────────────────────────────┘
```

三个核心结构：

- `refTable`：分页数组，热路径 `ref -> *writeSeries`
- `hashIndex`：分片 map，冷路径 `hash -> ref`
- `labelCatalog`：labels 外移存储，`writeSeries` 只持 `labelsID`

外部依赖只有三块：`ChunkDiskMapper`、WAL、`compactor.Write()`。

---

## 4. 核心数据结构

这部分只有一个原则：**热对象尽量瘦，冷信息尽量外移。**

### 4.1 `refTable`：ref 分页数组

`refTable` 是热路径主索引。**因为 `ref` 是递增整数，所以这里直接用分页数组，不用 `map`。**

```go
const (
    pageShift = 14              // 每页 16384 条
    pageSize  = 1 << pageShift
    pageMask  = pageSize - 1
)

type refTable struct {
    mu    sync.RWMutex
    pages []*refPage
}

type refPage struct {
    entries [pageSize]*writeSeries
}

func (t *refTable) get(ref chunks.HeadSeriesRef) *writeSeries {
    pageIdx := uint64(ref) >> pageShift
    t.mu.RLock()
    if pageIdx >= uint64(len(t.pages)) {
        t.mu.RUnlock()
        return nil
    }
    p := t.pages[pageIdx]
    t.mu.RUnlock()
    if p == nil {
        return nil
    }
    return p.entries[uint64(ref)&pageMask]
}
```

`HeadSeriesRef` 是单调递增整数，用分页数组可以减少 `map` 的 bucket 和指针开销。

### 4.2 `hashIndex`：分片冷路径 map

`hashIndex` 只服务冷路径。**它的职责很简单：在没有 `ref` 时，帮你从 labels/hash 找回 series。**

```go
const stripeSize = 256

type hashIndex struct {
    locks   [stripeSize]sync.RWMutex
    buckets [stripeSize]map[uint64][]refEntry
}

type refEntry struct {
    ref      chunks.HeadSeriesRef
    labelsID uint32
}
```

发生 hash 冲突时，通过 `labelsID` 回读 labels 做校验，槽内不直接存 labels。

### 4.3 `labelCatalog`：labels arena

labels 不直接挂在 `writeSeries` 上，而是统一放到 `labelCatalog`。**这样每条 series 只保留一个 `labelsID`。**

```go
type labelCatalog struct {
    mu    sync.RWMutex
    arena []byte      // 扁平编码：[len][name][len][value]...
    index []uint32    // labelsID -> arena offset
}
```

- `arena` append-only，不做单条 GC；Head truncate 时整段重建回收
- `writeSeries` 只持 `labelsID`（4 字节）
- 创建 series、WAL `RefSeries`、flush 排序时才回读 labels

### 4.4 `writeSeries`：最小写入状态

`writeSeries` 只保留“继续写下一条样本”必须用到的字段。**事务、查询、旧 chunk 链这些都不要。**

```go
type writeSeries struct {
    mu sync.Mutex

    ref      chunks.HeadSeriesRef
    labelsID uint32

    lastTs  int64
    lastVal float64

    // openChunk 按需分配；无样本时为 nil
    openChunk chunkenc.Chunk
    openApp   chunkenc.Appender
    openMinT  int64
    nextAt    int64

    // sealed 定长数组，窗口内通常 ≤ 2；超过 4 强制 flush
    sealedCount uint8
    sealed      [4]sealedChunkMeta
}

type sealedChunkMeta struct {
    chunkRef chunks.ChunkDiskMapperRef
    minTime  int64
    maxTime  int64
}
```

这里不引入 `txRing`、`pendingCommit`、`mmappedChunks` 链、OOO 状态等字段。

---

## 5. 写入路径

写入路径的核心也很简单：**有 `ref` 就直达，没有 `ref` 才走 hash 查找；写入过程中只维护最小状态，并把样本写进 WAL。**

### 5.1 `Append`

```go
func (a *appender) Append(ref SeriesRef, lset labels.Labels, t int64, v float64) (SeriesRef, error) {
    head := a.head
    s := head.refTable.get(chunks.HeadSeriesRef(ref))

    if s == nil {
        // 冷路径：hash 查
        hash := lset.Hash()
        s = head.hashIndex.get(hash, lset, head.labelCatalog)
        if s == nil {
            // 新建：分配 ref、写 catalog、两张索引都注册
            s = head.createSeries(hash, lset)
            a.pendingNewSeries = append(a.pendingNewSeries, s)
        }
    }

    s.mu.Lock()
    if t <= s.lastTs {
        s.mu.Unlock()
        return 0, storage.ErrOutOfOrderSample
    }
    if s.openChunk == nil {
        s.openNewChunk(t, head.chunkRange)
    } else if t >= s.nextAt || s.openChunk.NumSamples() >= samplesPerChunk {
        s.cutAndSpill(head.chunkDisk)
    }
    s.openApp.Append(t, v)
    s.lastTs, s.lastVal = t, v
    s.mu.Unlock()

    a.pendingSamples = append(a.pendingSamples, record.RefSample{Ref: s.ref, T: t, V: v})
    a.pendingSeries  = append(a.pendingSeries, s)
    return SeriesRef(s.ref), nil
}
```

热路径只经过 `refTable.get` 和 `writeSeries.mu`，不维护 `postings`，也不碰全局查询结构。

### 5.2 `Commit` / `Rollback`

提交时先把元数据和样本落 WAL，再更新窗口并判断是否 flush。回滚时沿用 `agent` 语义。

`Commit` 分四步：

1. 写新 series 的 `RefSeries` WAL
2. 写本批次样本的 `RefSample` WAL
3. 更新 Head 时间窗
4. 判断是否触发 flush

`Rollback` 需要保留新建 series 的 `Series` 记录，避免后续样本引用一个 WAL 中不存在的 ref。

另外，`Append()` 阶段拿到的 `*writeSeries` 必须直接保存在 appender 私有切片里，`Commit()` 不能回到全局目录重查。

### 5.3 chunk 切分与落盘

**内存里只保留当前正在写的 chunk。一个 chunk 一旦封口，就立刻写到磁盘。**

切分语义沿用现有 `memSeries.appendPreprocessor()`：初次写、时间边界 `nextAt`、样本数上限、编码变化都会触发切分。

切分时做四件事：

1. 取出当前 `openChunk`
2. 调 `chunkDiskMapper.WriteChunk(...)` 写入 head chunk 文件
3. 把 `(chunkRef, minTime, maxTime)` 追加到 `sealed[]`
4. 清空 `openChunk`，等待下一条样本再懒分配

这样 sealed chunk 的字节会立刻离开 heap，`writeSeries` 里只留下最小元数据。

---

## 6. Flush

**平时不维护查询结构，等到 flush 时再临时把一个时间窗口整理成 block。**

时间窗触发策略沿用现有 Head：当 `MaxTime - MinTime > 1.5 * chunkRange`，从左侧切出按 `chunkRange` 对齐的窗口做 flush。

执行路径：

```go
func (h *WriteOnlyHead) Flush(mint, maxt int64) error {
    br := &writeHeadBlockReader{head: h, mint: mint, maxt: maxt}
    _, err := h.compactor.Write(h.dir, br, mint, maxt, nil)
    if err != nil {
        return err
    }
    h.truncate(maxt)
    return nil
}
```

`writeHeadBlockReader` 只在 flush 瞬间存在，主要做四件事：

- 扫描窗口内的 sealed chunk metadata
- 按 `labelsID` 回读 labels
- 排序并构造写 block 所需的临时索引视图
- 直接引用 `ChunkDiskMapper` 里的 chunk 字节

flush 完成后这些临时结构立即丢弃，所以 **steady-state 不存在查询形态的常驻结构**。代价是 flush 阶段会有一次性 CPU 和短时内存峰值。

---

## 7. WAL replay

重启恢复时只重建映射关系和最后时间戳，**不重建 open chunk。**

replay 只做两件事：

1. 重建 `refTable` / `hashIndex` / `labelCatalog` 的 `ref -> writeSeries` 映射
2. 恢复每条 series 的 `lastTs`

这样启动时不会给冷 series 预留 chunk 内存。下一条样本到来时，再懒分配 `openChunk`。

---

## 8. 代码组织

建议单独落一组新文件，不直接改造现有 `Head`。**这样更容易并行开发、做 AB 对比，也更不容易把老逻辑搅乱。**

| 文件 | 内容 | 预计行数 |
|------|------|---------|
| `tsdb/write_head.go` | 结构体 + 生命周期 | ~300 |
| `tsdb/write_head_append.go` | appender | ~250 |
| `tsdb/write_head_series.go` | refTable + hashIndex | ~200 |
| `tsdb/write_head_catalog.go` | labelCatalog | ~150 |
| `tsdb/write_head_chunks.go` | cut/spill | ~200 |
| `tsdb/write_head_flush.go` | blockReader 视图 + 调用 compactor.Write | ~300 |
| `tsdb/write_head_replay.go` | WAL 回放 | ~200 |
| **合计** | | **~1600 行** |

补充一点：

- 通过独立 option（如 `StorageMode = FullHead | WriteOnlyHead`）切换
- 不替换 `Head` 默认行为
- 尽量复用现有能力：`storage.Appender`、WAL 记录格式、`ChunkDiskMapper`、`chunkenc`、`compactor.Write()`、block 目录格式

---

## 9. 优化前后内存占用对比

在 **4000 万 series** 的目标场景下，Head 常驻内存预计可以从 **~39 GB** 降到 **~10.7 GB**，降幅约 **72%**。

以下估算只看 Head steady-state heap，不含 flush 瞬时峰值；假设平均 10 个 label，默认 `stringlabels` 构建，且大部分写入会复用 `ref`。

| 内存构成 | 现有 Head | WriteOnlyHead | 说明 |
|---------|----------|---------------|------|
| `postings`（倒排索引 + 全量 postings） | ~10 GB | **0** | 完全不维护 |
| series 主索引（`stripeSeries` 双 map） | ~6 GB | **~1.5 GB** | 分页数组替代 map，去掉 hash 索引里的 `*memSeries` 指针 |
| labels 本体 | ~6 GB | **~3.2 GB** | `labelCatalog` arena 编码 + `labelsID` 外移 |
| series 对象本身 | ~12 GB | **~5 GB** | `memSeries` → `writeSeries`，字段数从 ~25 降到 ~11 |
| head chunks（含 mmappedChunks 链） | ~5 GB | **~1 GB** | 只保留 openChunk，sealed 立即落盘；openChunk 按需懒分配 |
| **合计** | **~39 GB** | **~10.7 GB** | **降幅 ~72%** |

收益主要来自五处：

- **去掉 `postings`**：write-only 不做标签查询，这部分可以整块移除
- **压缩主索引**：`map[ref]*writeSeries` 改为分页数组；`hashIndex` 只存 `(ref, labelsID)`
- **外移 labels**：labels 统一放到 arena，`writeSeries` 只留 `labelsID`
- **瘦身 `writeSeries`**：去掉事务、查询、OOO 和旧 chunk 链等字段
- **及时落盘 sealed chunk**：内存只保留 openChunk

收益会被削弱的场景：

- **调用方不复用 `ref`**：冷路径热化，`hashIndex` 和 `labelCatalog` 的访问会变多
- **所有 series 持续活跃**：每条 series 都会长期占用 openChunk
- **label 特别长或特别多**：`labelCatalog` arena 会变大
- **series churn 很高**：会持续创建新的 `writeSeries` 和 `labelsID`

---

## 10. 风险与权衡

这个方案不复杂，但有三个前提不能忽略：**调用方要复用 `ref`，flush 会更重，replay 要做严谨。**

- **调用方 `ref` 复用率**决定收益上限
- **flush 复杂度上移**：平时不维护查询结构，flush 时要临时整理 block 视图
- **WAL replay / crash recovery 需要仔细设计**：`refTable`、`labelCatalog`、sealed chunk metadata 的重建必须和 WAL 语义严格对齐

---

## 11. 实施范围

本方案只做写入闭环，不做 Head 查询。**凡是需要查询的需求，都应该去读 flush 后的 block。**

本方案包含：

- in-order float 样本写入
- `refTable` / `hashIndex` / `labelCatalog`
- chunk 切分 + sealed spill 到 `ChunkDiskMapper`
- flush 走 `writeHeadBlockReader` + `compactor.Write()`
- WAL + replay（不重建 openChunk）
- 基础监控：active series 数、refTable 大小、hashIndex 命中率、ref hit 比例、chunk spill 次数、flush 耗时、WAL replay 耗时

验收标准：

- 能持续 ingest
- 能 WAL replay
- 能 flush 出合法 block
- steady-state 内存显著低于现有 Head

**架构红线：Head 查询、`postings`、label 查询都不在范围内。** 如果为了查询再把索引加回去，这个方案的内存收益模型就失效了。

---

## 12. 一句话总结

**这是一个只为写入服务的轻量 Head。它把查询结构留到 flush 时再临时整理，因此能明显降低常驻内存。**

在 4000 万 series 场景下，Head 常驻内存预计可从 **~39 GB** 降到 **~10.7 GB**，降幅约 **72%**。

---

## 13. 术语说明

- **`ref`**：series 在 Head 内部的整数 ID。调用方缓存它后，后续写入可以直接定位到目标 series。
- **`ref-first`**：优先用 `ref` 写入，而不是每次都根据 labels 做 hash 和查找。
- **steady-state**：系统进入平稳运行后的常态。这里主要指非启动、非 replay、非 flush 瞬时峰值时的常驻内存状态。
- **`postings`**：为标签查询服务的倒排索引。WriteOnlyHead 不维护这部分。
- **open chunk**：当前仍在追加样本的 chunk。
- **sealed chunk**：已经切分完成、不再追加的 chunk。
- **spill**：把 sealed chunk 的字节从内存写到 `ChunkDiskMapper`，只在内存里留下最小元数据。
- **flush**：把一个时间窗口内的数据整理成 block 并写到磁盘。
- **WAL**：Write-Ahead Log。样本和 series 先写 WAL，再视情况 flush 成 block。
- **WAL replay**：进程重启后重放 WAL，用来恢复内存状态。
- **arena**：一块连续的内存区域。这里用于紧凑存储 labels，减少零散对象和指针开销。
- **`BlockReader`**：写 block 时需要的读取视图接口。这里不是给查询用，而是给 flush 阶段临时组装 block 用。
- **OOO**：Out-Of-Order，指时间戳乱序写入。本文方案默认只处理 in-order 写入。
