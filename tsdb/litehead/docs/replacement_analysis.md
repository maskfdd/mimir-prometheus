# litehead 替换标准 tsdb.Head 方案分析

> 目标：在 mimir-ingester 等"只写 + 定期 flush"场景中，用 `litehead.Head` 替换 `tsdb.Head`，
> 不破坏 `tsdb.DB` 的对外行为，同时大幅降低内存。

---

## 一、现状：`tsdb.DB` 与 `tsdb.Head` 的耦合方式

### 1.1 字段持有方式

```go
// tsdb/db.go:264
type DB struct {
    ...
    head *Head   // 具体类型，非接口
    ...
}
```

`db.Head()` 返回 `*Head`（具体类型），外部调用者直接拿到标准 Head 的全部方法。

### 1.2 DB 对 Head 的调用清单（按功能分组）

#### A. 公共方法（litehead 已有或可适配）

| 方法 | DB 中用法 | litehead 现状 |
|---|---|---|
| `Appender(ctx)` | `db.Appender()` 代理 | ✅ 已有 |
| `MinTime()` | 查询/Compact/metrics 多处 | ✅ 已有 |
| `MaxTime()` | 查询/Compact/Snapshot | ✅ 已有 |
| `NumSeries()` | metrics | ✅ 已有 |
| `Meta()` | RangeHead | ✅ 已有 |

#### B. 公共方法（litehead 需要新增/适配）

| 方法 | DB 中用法 | litehead 建议 |
|---|---|---|
| `Init(minValidTime int64)` | `db.go:966` 初始化 + 设 minValidTime | ⚠️ litehead 有 `Init()` 但无参数 → 需加 `minValidTime` 参数 |
| `ApplyConfig(conf, wblog)` | `db.go:1129` OOO/native histograms 开关 | ⚠️ litehead 无 OOO/WBL → 空实现即可 |
| `EnableNativeHistograms()` | `db.go:1139` | ❌ 需新增（可空实现） |
| `DisableNativeHistograms()` | `db.go:1144` | ❌ 需新增（可空实现） |
| `WaitForAppendersOverlapping(maxt)` | `db.go:1228` Compact 前等待 | ❌ 需新增（litehead 无 isolation，可 noop 或简单 barrier） |
| `IsQuerierCollidingWithTruncation(mint, maxt)` | `db.go:1964,2038` 查询路径 | ❌ litehead 不支持查询 → 返回 `(false, false, 0)` |
| `Truncate(maxt int64)` | `db.go:1451` 按时间截断 | ❌ 需新增（可代理到内部 `truncateMemory`） |
| `OverlapsClosedInterval(mint, maxt)` | `db.go:2132` Delete 路径 | ❌ 需新增（litehead 不支持 Delete → 返回 false） |
| `Delete(ctx, mint, maxt, ms...)` | `db.go:2134` | ❌ litehead 写专用，返回 ErrUnsupported |
| `Stats(statsByLabel, limit)` | `cmd/prometheus/main.go:1509` | ✅ 已有（签名匹配） |
| `PostingsCardinalityStats(name, limit)` | 标准 Head.Stats 内部调用 | ✅ 已有（返回 nil） |
| `MinOOOTime()` / `MaxOOOTime()` | `db.go:984,1981,2055` OOO 查询 | ❌ 返回 `math.MaxInt64` / `math.MinInt64` 即可 |
| `ForEachSecondaryHash(fn)` | 可选 callback | ❌ 可空实现 |

#### C. 私有方法（DB 直接调用，最大障碍）

| 私有方法/字段 | DB 中用法 | 说明 |
|---|---|---|
| `head.compactable()` | `db.go:1168,1209` 触发 Compact | litehead 已有，需提升为公共方法或通过接口暴露 |
| `head.truncateWAL(maxt)` | `db.go:1196,1239,1271` | litehead 已有，需提升为公共方法 |
| `head.truncateMemory(maxt)` | `db.go:1390` | litehead 已有，需提升为公共方法 |
| `head.truncateOOO(lastWBLFile, minRef)` | `db.go:1310` OOO 路径 | litehead 无 OOO → 空实现 |
| `head.mmapHeadChunks()` | `db.go:1055,1899` 定时 mmap | litehead 不需要（sealed 即时 mmap） → 空实现 |
| `head.appendableMinValidTime()` | `db.go` Compact 相关 | litehead 已有，需提升为公共方法或由 AppendableMinValidTime() 代替 |
| `head.chunkRange.Load()` | `db.go:1213,1244,1248` 直接读原子量 | litehead 已有 `ChunkRange()` 方法 |
| `head.writeNotified` | `db.go:941,2196` 设置 write notify | litehead 目前没有此字段 |
| `head.wal` | `db.go` WAL 相关 | litehead 内部有 WAL，但 DB 不应直接访问 |
| `head.wbl` | `db.go:1112-1114` OOO WBL | litehead 无 WBL → 不需要 |
| `head.metrics.walCorruptionsTotal` | `db.go:967` | litehead 有自己的 metrics，需暴露或接口化 |
| `head.exemplars` | `db.go:2108` ExemplarQuerier | litehead 不支持 exemplar → 返回 ErrUnsupported |
| `head.iso` | RangeHead 通过 `head.iso.State()` | litehead 无 isolation → 不需要 |

#### D. `RangeHead` / `OOORangeHead` 的耦合

```go
// tsdb/head.go:1390
type RangeHead struct {
    head       *Head    // 具体类型！
    mint, maxt int64
}
```

`RangeHead` 直接持有 `*Head`，并调用：
- `head.indexRange(mint, maxt)` — 私有方法
- `head.chunksRange(mint, maxt, isoState)` — 私有方法
- `head.iso.State(mint, maxt)` — 私有字段
- `head.tombstones` — 私有字段
- `head.Meta().ULID`

**litehead 不需要 RangeHead**：litehead 的 flush 路径通过自己的 `blockReader` 直接实现了 `tsdb.BlockReader`，不走 `RangeHead`。

---

## 二、替换方案对比

### 方案 A：提取 HeadLike 接口（推荐 ✅）

**核心思路**：在 `tsdb` 包中定义一个 `HeadLike` 接口，包含 DB 所需的所有方法，让标准 Head 和 litehead 都实现它。

```go
// tsdb/head_interface.go（新文件）
package tsdb

type HeadLike interface {
    // --- 基础属性 ---
    MinTime() int64
    MaxTime() int64
    NumSeries() uint64
    Meta() BlockMeta
    ChunkRange() int64
    Size() int64

    // --- 写入 ---
    Appender(ctx context.Context) storage.Appender

    // --- 生命周期 ---
    Init(minValidTime int64) error
    Close() error

    // --- Compaction ---
    IsCompactable() bool
    AppendableMinValidTime() int64
    TruncateWAL(maxt int64) error
    TruncateMemory(maxt int64) error
    MmapHeadChunks()
    WaitForAppendersOverlapping(maxt int64)

    // --- 配置 ---
    ApplyConfig(conf *config.Config, wblog *wlog.WL)
    EnableNativeHistograms()
    DisableNativeHistograms()
    SetWriteNotified(wn wlog.WriteNotified)

    // --- 查询（可选，litehead 返回 ErrUnsupported）---
    IsQuerierCollidingWithTruncation(mint, maxt int64) (shouldClose, getNew bool, newMint int64)
    OverlapsClosedInterval(mint, maxt int64) bool
    Delete(ctx context.Context, mint, maxt int64, ms ...*labels.Matcher) error

    // --- OOO（litehead 返回零值）---
    MinOOOTime() int64
    MaxOOOTime() int64
    TruncateOOO(lastWBLFile int, minMmapRef uint64) error

    // --- 统计 ---
    Stats(statsByLabelName string, limit int) *Stats
    PostingsCardinalityStats(statsByLabelName string, limit int) *index.PostingsStats

    // --- Exemplar ---
    ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

    // --- Metrics（DB 的 init 路径需要）---
    IncrementWALCorruptionsTotal()

    // --- 查询侧 BlockReader（仅标准 Head 提供，litehead 返回自己的 blockReader）---
    RangeBlockReader(mint, maxt int64) BlockReader
}
```

**DB 改造**：

```go
// db.go
type DB struct {
    ...
    head HeadLike  // 接口替换具体类型
    ...
}

// Head() 返回接口而非具体类型
func (db *DB) Head() HeadLike {
    return db.head
}
```

**改造要点**：

1. **DB 不再直接访问私有字段/方法**：所有 `head.compactable()` → `head.IsCompactable()`，`head.truncateWAL()` → `head.TruncateWAL()`，`head.chunkRange.Load()` → `head.ChunkRange()`，等等。

2. **RangeHead 改造**：DB 中 `NewRangeHead(db.head, mint, maxt)` 改为 `db.head.RangeBlockReader(mint, maxt)` — 标准 Head 内部仍创建 `RangeHead`，litehead 返回自己的 `blockReader`。

3. **writeNotified** → 通过 `SetWriteNotified(wn)` 方法注入。

4. **metrics.walCorruptionsTotal** → 通过 `IncrementWALCorruptionsTotal()` 方法暴露。

5. **head.wbl / head.wal** → DB 不再直接访问；标准 Head 在 `ApplyConfig` 中自行处理 WBL。

**litehead 需要新增的方法**（大部分空实现）：

```go
// litehead 补充方法
func (h *Head) Init(minValidTime int64) error {
    h.minValidTime.Store(minValidTime)
    return h.replayAndRecover() // 复用现有 Init 逻辑
}

func (h *Head) TruncateWAL(maxt int64) error    { return h.truncateWAL(maxt) }
func (h *Head) TruncateMemory(maxt int64) error  { h.truncateMemory(maxt); return nil }
func (h *Head) MmapHeadChunks()                  {} // noop：litehead 即时 mmap
func (h *Head) WaitForAppendersOverlapping(int64) {} // noop：无 isolation
func (h *Head) ApplyConfig(*config.Config, *wlog.WL) {} // noop：无 OOO/WBL
func (h *Head) EnableNativeHistograms()           {} // noop 或按需支持
func (h *Head) DisableNativeHistograms()          {} // noop
func (h *Head) SetWriteNotified(wn wlog.WriteNotified) {} // noop 或存储

func (h *Head) IsQuerierCollidingWithTruncation(mint, maxt int64) (bool, bool, int64) {
    return false, false, 0
}
func (h *Head) OverlapsClosedInterval(mint, maxt int64) bool { return false }
func (h *Head) Delete(ctx context.Context, mint, maxt int64, ms ...*labels.Matcher) error {
    return ErrQuerierUnsupported
}

func (h *Head) MinOOOTime() int64 { return math.MaxInt64 }
func (h *Head) MaxOOOTime() int64 { return math.MinInt64 }
func (h *Head) TruncateOOO(int, uint64) error { return nil }

func (h *Head) ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error) {
    return nil, ErrQuerierUnsupported
}
func (h *Head) IncrementWALCorruptionsTotal() { h.metrics.walCorruptionsTotal.Inc() }

func (h *Head) RangeBlockReader(mint, maxt int64) tsdb.BlockReader {
    return newBlockReader(h, mint, maxt) // 复用现有 blockReader
}
```

**优点**：
- 改动集中、可逐步推进
- 标准 Head 也实现 HeadLike，不影响现有用户
- litehead 不需要 fork DB
- 可独立测试接口兼容性

**缺点**：
- 接口较大（~25 个方法）
- 标准 Head 需要把部分私有方法提升为公共方法
- 外部调用者（如 `cmd/prometheus/main.go`）依赖 `db.Head()` 返回的具体类型

---

### 方案 B：Adapter / Wrapper 模式

不改 DB，在外层包装：

```go
type LiteDB struct {
    dir     string
    head    *litehead.Head
    blocks  []*tsdb.Block
    compactor tsdb.Compactor
}
```

直接在 `LiteDB` 里实现 `storage.Storage` + 自行管理 block 生命周期。

**优点**：
- 不碰 `tsdb.DB` 和标准 `Head` 的代码
- litehead 完全独立

**缺点**：
- 需要重新实现 block 管理、retention、compaction 调度等逻辑（`db.go` 的 ~2000 行代码）
- 维护成本高，容易跟上游 drift

---

### 方案 C：Fork DB 为 LiteDB（不推荐）

直接复制 `db.go` 为 `litedb.go`，把 `*Head` 替换为 `*litehead.Head`，删除不需要的路径。

**优点**：快速可用
**缺点**：代码重复，长期维护灾难

---

## 三、推荐方案：A（接口化）分步实施计划

### Phase 1：在标准 Head 上完成公共化（不碰 litehead）

**改动范围**：`tsdb/` 包内

1. **新建 `tsdb/head_interface.go`**：定义 `HeadLike` 接口。
2. **标准 Head 补齐公共方法**：
   - `compactable()` → 新增 `IsCompactable() bool` 公共方法（或直接改名）
   - `truncateWAL()` → 新增 `TruncateWAL()` 公共方法
   - `truncateMemory()` → 新增 `TruncateMemory()` 公共方法
   - `truncateOOO()` → 新增 `TruncateOOO()` 公共方法
   - `mmapHeadChunks()` → 新增 `MmapHeadChunks()` 公共方法
   - `appendableMinValidTime()` → 已有 `AppendableMinValidTime()`? 检查并确认
   - 新增 `SetWriteNotified(wn)`
   - 新增 `IncrementWALCorruptionsTotal()`
   - 新增 `RangeBlockReader(mint, maxt int64) BlockReader`
3. **确认标准 Head 满足 HeadLike 接口**：`var _ HeadLike = (*Head)(nil)`
4. **此阶段不改 DB 结构**，标准 Head 同时保留私有方法（向后兼容）。

### Phase 2：DB 改用 HeadLike 接口

**改动范围**：`tsdb/db.go` + `tsdb/querier.go`

1. **`db.head` 类型改为 `HeadLike`**。
2. **DB 中所有 `head.xxx` 私有调用改为公共方法调用**：
   - `head.compactable()` → `head.IsCompactable()`
   - `head.truncateWAL(x)` → `head.TruncateWAL(x)`
   - `head.chunkRange.Load()` → `head.ChunkRange()`
   - `head.writeNotified = x` → `head.SetWriteNotified(x)`
   - `head.metrics.walCorruptionsTotal.Inc()` → `head.IncrementWALCorruptionsTotal()`
   - `NewRangeHead(db.head, ...)` → `db.head.RangeBlockReader(mint, maxt)`
   - 等等（约 37 处调用需修改）
3. **处理 OOO 路径**：
   - `NewOOOCompactionHead(ctx, db.head)` → 需要 type assert：`if stdHead, ok := db.head.(*Head); ok { ... }`
   - 或在 HeadLike 中加 `OOOCompactionHead(ctx) (*OOOCompactionHead, error)`
4. **`db.Head()` 返回类型**改为 `HeadLike`。
   - ⚠️ **外部调用者可能需要适配**（`cmd/prometheus/main.go` 等）

### Phase 3：litehead 实现 HeadLike

**改动范围**：`tsdb/litehead/`

1. **修改 `Init()` 签名** 为 `Init(minValidTime int64) error`。
2. **补齐所有 HeadLike 方法**（大部分为 noop，见上文代码片段）。
3. **确认**：`var _ tsdb.HeadLike = (*Head)(nil)`。
4. **写单测**：验证所有 noop 方法返回预期值。

### Phase 4：集成测试

1. **在 mimir-ingester 中注入 litehead**：
   ```go
   // 初始化路径选择
   if cfg.UseLiteHead {
       lh, _ := litehead.NewHead(logger, reg, dir, liteOpts)
       db = tsdb.OpenDBWithHead(dir, logger, reg, dbOpts, lh)
   } else {
       db = tsdb.Open(dir, logger, reg, dbOpts, nil)
   }
   ```
2. **需要在 `tsdb.DB` 中新增构造函数**：`OpenDBWithHead(dir, logger, reg, opts, head HeadLike) (*DB, error)`，允许外部注入 Head。
3. **测试覆盖**：
   - 基本写入 → flush → block 生成
   - WAL replay
   - Compaction 循环
   - Retention（时间/空间）
   - Graceful shutdown（snapshot + final flush）

### Phase 5：外部调用者适配

| 调用方 | 当前调用 | 适配方式 |
|---|---|---|
| `cmd/prometheus/main.go` | `db.Head().MinTime()` | ✅ HeadLike 有此方法 |
| `cmd/prometheus/main.go` | `db.Head().MaxTime()` | ✅ HeadLike 有此方法 |
| `cmd/prometheus/main.go` | `db.Head().Stats(...)` | ✅ HeadLike 有此方法 |
| `web/` | `db.Head()` for status pages | 可能需 type assert 展示额外信息 |
| `storage/remote/` | test 中用 `db.Head()` | 需适配接口 |

---

## 四、litehead 独有的优势与限制

### 替换后获得的好处

1. **内存大幅下降**：无 postings、无 isolation、无 exemplar storage、无 OOO 结构
2. **GC 压力降低**：series 结构精简，堆对象更少
3. **写路径更快**：无 isolation append ID 分配、无 postings 更新
4. **flush 自包含**：litehead 自带 `blockReader` 直接喂 compactor，无需 `RangeHead` + `indexRange` + `chunksRange`

### litehead 不支持的功能（替换后丧失）

| 功能 | 影响 |
|---|---|
| **查询 Head 中的数据** | ingester 场景通过查询 flushed blocks 解决 |
| **OOO 写入** | Mimir ingester 已通过分布式保证顺序 |
| **Exemplar** | 需要 exemplar 的场景不适用 litehead |
| **Delete** | ingester 场景不需要 |
| **Isolation（事务隔离）** | litehead 无并发查询，不需要 |
| **Tombstones** | litehead 无删除 |
| **Native Histogram** | 当前 litehead appender 不支持，需按需扩展 |

---

## 五、需要注意的坑

### 5.1 `db.go` 中 `Compact()` 的 RangeHead 路径

```go
// db.go:1222
rh := NewRangeHeadWithIsolationDisabled(db.head, mint, maxt-1)
```

标准 Head 的 `Compact()` 用 `RangeHead` 包装 Head 后喂给 `compactor.Write()`。litehead 替换后：
- **不能复用这个路径**，因为 litehead 没有 `indexRange()` / `chunksRange()` 等私有方法。
- **解决**：litehead 的 `RangeBlockReader(mint, maxt)` 返回自己的 `blockReader`，DB 直接用这个。

### 5.2 `OOOCompactionHead`

```go
// db.go:1289
oooHead, err := NewOOOCompactionHead(ctx, db.head)
```

`NewOOOCompactionHead` 入参是 `*Head`（具体类型），litehead 不支持。

**解决**：
- DB 在 OOO compaction 路径做 type assert：
  ```go
  if stdHead, ok := db.head.(*Head); ok {
      oooHead, err = NewOOOCompactionHead(ctx, stdHead)
  }
  ```
- 或者 `HeadLike` 中加 `OOOCompactionHead` 方法，litehead 返回 nil。

### 5.3 `dbAppender.Commit()` 触发 Compact

```go
// db.go:1168
if a.db.head.compactable() {
```

litehead 下这个 `IsCompactable()` 需要正确反映状态。litehead 已有 `compactable()` 逻辑，
但 DB 的 `Compact()` 会调用 `RangeBlockReader()` → litehead 自己的 compaction 路径。

**注意**：litehead 自带 `Flush()`（`tryFlushAll`），与 DB 的 `Compact()` 可能**重复触发**。需要决定：
- **方案 A**（推荐）：litehead 模式下 DB 的 `Compact()` 直接代理到 `litehead.Flush()`，不走 RangeHead 路径。
- **方案 B**：litehead 的 `IsCompactable()` 返回 false，flush 由外部定时器独立驱动。

### 5.4 `head.Init(minValidTime)` 的语义差异

标准 Head：
```go
func (h *Head) Init(minValidTime int64) error {
    // 设置 minValidTime
    // 回放 WAL/WBL/Snapshot
    // 处理 mmapped chunks
}
```

litehead：
```go
func (h *Head) Init() error {
    // 回放 ChunkDiskMapper
    // 回放 WAL
}
```

litehead 需要修改 `Init` 签名，接受 `minValidTime` 参数并 store 到 `h.minValidTime`。

### 5.5 外部调用者的类型断言

`cmd/prometheus/main.go:1509`：
```go
return db.Head().Stats(statsByLabelName, limit), nil
```

如果 `db.Head()` 返回 `HeadLike`，`Stats()` 在接口中有定义，这行不需要改。

但如果有调用者依赖 `*Head` 特有的方法（如 `ForEachSecondaryHash`），则需要 type assert。

---

## 六、工作量估算

| Phase | 改动文件数 | 估算工时 | 风险 |
|---|---|---|---|
| Phase 1：标准 Head 公共化 | ~5 文件 | 2-3 天 | 低（纯增量） |
| Phase 2：DB 接口化 | ~3 文件 | 3-5 天 | 中（37 处调用修改） |
| Phase 3：litehead 实现 HeadLike | ~2 文件 | 1-2 天 | 低（大部分 noop） |
| Phase 4：集成测试 | ~3 文件 | 2-3 天 | 中 |
| Phase 5：外部调用者适配 | ~4 文件 | 1-2 天 | 低 |
| **合计** | | **9-15 天** | |

---

## 七、替代选择：litehead 独立运行（不走 DB）

如果不想改 `tsdb.DB`，litehead 已经实现了 `storage.Storage` 接口，**可以完全绕过 DB**：

```go
// mimir-ingester 直接使用
var store storage.Storage = litehead.NewHead(...)
store.Init()
defer store.Close()

appender := store.Appender(ctx)
// ... 写入 ...
appender.Commit()

// 定期 flush
store.(*litehead.Head).Flush()
```

但这意味着需要自行处理：
- Block 管理（reload、retention、compaction 已有 block 的合并）
- 对外暴露 `Querier` 时需要合并 litehead blocks + 旧 blocks

**适用场景**：Mimir ingester 本身已有上层 block 管理逻辑，litehead 只负责写入 + flush，
block 的生命周期由 ingester/store-gateway 管理。**这种情况下无需改动 `tsdb.DB`**。

---

## 八、总结与建议

| 场景 | 推荐方案 |
|---|---|
| Mimir ingester（已有上层 block 管理） | **直接使用 litehead 作为 `storage.Storage`**，不走 `tsdb.DB` |
| 通用替换（需要 DB 的 block 管理、retention 等） | **方案 A：HeadLike 接口化**，分 5 个 Phase 逐步推进 |
| 快速原型验证 | 方案 B：独立 LiteDB wrapper |

**如果目标仅是 mimir-ingester 场景**，litehead 已经具备独立运行的能力（`storage.Storage`），
最简路径是在 ingester 层直接注入 litehead，无需改动 `tsdb.DB`。只有需要复用 DB 的
block retention / compaction / querier 合并等完整功能时，才需要走接口化路线。
