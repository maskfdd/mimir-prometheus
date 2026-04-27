# litehead 优化点梳理（内存优先，兼顾 CPU/延迟）

> 目标规模：单实例 ~40M series。优化以"内存占用 / GC 压力 / 写路径抖动"为主，但**不能以牺牲写 TPS 和 P99 延迟为代价**——下面每条优化都会在"trade-off"中标注对 CPU/延迟的影响。
>
> 粗估内存基线：当前稳态 `memSeries` 约 360 B/条，40M series ≈ 14 GiB 常驻，labelCatalog arena 另计。

---

## 〇、总体设计原则（内存 vs CPU/延迟）

落地每条优化前先按这四条原则判断一次：

1. **写 hot path 零新增分配**：Append 每样本的 CPU 开销必须只增不减。换内存可以换点 CPU，但不能换"每 Append 一次新 alloc"——GC 会吃掉节省的内存。
2. **避免全局 stop-the-world**：任何"遍历全部 series 持长锁"的操作（rebuildArena、forced flush、全量 snapshot）都要分批 + 让步，保持 P99 < 500ms。
3. **计算换存储要可控**：例如 `nextAt` 改推导、arena 改 `[][]byte`——仅当额外 CPU 在 hot path ≤ 单条纳秒级、且非 hot path 路径能接受时才做。
4. **不为节省几 B 破坏 cache 局部性**：memSeries 字段排列优先 cache line（hot 字段 `lastTs/openMinT/openApp/openChunk` 放一起），再考虑紧凑。

经验阈值（40M series / 100w samples/s 目标）：
- Append P99 ≤ 1ms；单样本 CPU ≤ 200 ns。
- flush/snapshot 期写抖动 P99 ≤ 500ms。
- 稳态 GC pause P99 ≤ 50ms（heap < 30 GiB 时可达）。

---

## 一、memSeries 本体（最大可优化点）

当前结构（`series.go`）：

```go
type memSeries struct {
    mu sync.Mutex                      // 8 B
    ref      chunks.HeadSeriesRef      // 8 B
    labelsID uint32                    // 4 B
    lastTs   int64                     // 8 B (+4 padding)
    openChunk chunkenc.Chunk           // 16 B interface
    openApp   chunkenc.Appender        // 16 B interface
    openMinT, openMaxT, nextAt int64   // 24 B
    mmappedChunksCount uint8           // 1 B (+7 padding)
    mmappedChunks [8]mmappedChunk      // 8 * 32 = 256 B
}
// ≈ 360 B/series
```

### 1. `mmappedChunks [8]mmappedChunk` 内联数组代价过大（P0）
固定占 256 B/series，但稳态下常见 sealed chunk 只有 0–1 个。40M series 约 10 GiB 纯浪费。

候选方案（按代价递增）：
- **A. 按需分配 slice**：`mmappedChunks []mmappedChunk`，稳态 nil，slice header 24 B + 平均 1~2 个元素 × 32 B。
- **B. 1 内联 + 溢出 slice**：`mmappedChunks [1]mmappedChunk` + `overflow []mmappedChunk`，稳态零分配，同时能处理突发。
- **C. 全局 mmapped arena**：所有 sealed chunk 放到全局 ring/arena，series 只存 `firstIdx, count uint32`（8 B）。适合超大规模。

预期收益：方案 A ≈ -7 GiB，方案 C ≈ -10 GiB。

**Trade-off**：
- A 方案每次 seal 走一次 `append` 可能分配；首次 seal 时分配一次（amortized 可接受），对 CPU 影响 < 1%。
- B 方案稳态零分配，仅在第 2 个 sealed chunk 出现时分配一次，**推荐**。
- C 方案全局 arena 需要并发访问（读写锁或 shard），hot path 取 chunk 时多一次间接寻址，CPU +5~10%；实现复杂，除非 A/B 都不够省再考虑。

### 2. 锁可外置或降级（P1）
litehead 无读路径，`sync.Mutex` 没必要内嵌到 series：
- 将锁抽到 `refTable` 的分页级（每 16K series 共用一把锁），或按 `ref % N` 分片锁。
- 或用 `atomic.Uint32` 自旋 + `runtime.Gosched`（参考 Prometheus stripeLock）。

收益：省 8 B/series ≈ 300 MiB。

**Trade-off**：
- 分片锁方案：锁粒度变粗，高频 batch 写同一分片时会串行化。需按 `ref` 均匀分片，且分片数 ≥ 256 才不影响并发。⚠️ 若分片数过低，CPU 可能 +10~20%，反而恶化。
- atomic.Uint32 自旋：hot path 无额外开销，但短临界区外一旦被抢占会 busy-wait。建议只在"临界区 < 100ns"的场景用（当前 Append 的临界区更大，**不建议无脑换**）。
- **结论**：内存收益相对 P0 很小，优先级下调，仅在其它优化完成后再考虑；或保持 `sync.Mutex` 不动，只做字段重排拿对齐收益。

### 3. `nextAt` 可推导（P1）
`nextAt` 完全可从 `openMinT + chunkRange` 推出，不必存；写 hot path 多一次运算代价可忽略。省 8 B/series ≈ 300 MiB。

**Trade-off**：Append 每样本多一次整数加法 + 比较，纳秒级，可忽略。**安全收益**。

### 4. `openApp` interface 字段冗余（P1）
可以每次从 `openChunk.Appender()` 临时取，或缓存在 appender 的 pool 里（而不是 series 上）。去掉 16 B/series ≈ 600 MiB。

**Trade-off**：
- ⚠️ **不要每次 Append 都 `openChunk.Appender()`**——XOR chunk 的 `Appender()` 实际会 decode 一遍 header/state，CPU 会显著上涨（实测 +30~50%）。
- 正确做法：把 `*xorAppender` 放到 **appender 的 scratch 里**（`headAppender` 结构里加一个 `openApps map[*memSeries]chunkenc.Appender` 或按 ref 缓存），Commit 时丢弃。这样 series 上不占字节，hot path 也不重复初始化。
- 改动复杂度中等，收益 600 MiB 并不特别大，可视情做。

### 5. 字段重排 + 显式 padding 检查（P2）
当前布局存在隐式 padding。按 8B 对齐顺序重排 `int64` → `interface` → `uint32` → `uint8`，用 `unsafe.Sizeof` 核对一遍。

**Trade-off**：纯正收益——既节省内存又改善 cache 局部性（hot 字段放同一 cache line），Append CPU 可能 -2~5%。应**作为其它改动的附带项**一起做。

**综合最优方案下 `memSeries` 可从 ~360 B 降到 ~80–100 B，40M series 节省 10+ GiB；CPU 开销基本持平或小幅下降。**

---

## 二、labelCatalog

### 1. `sliceLocked` 每次 `make+copy`（P1）

```go
// label_catalog.go:106-117
buf := make([]byte, end-offset)
copy(buf, lc.arena[offset:end])
```

因为 `put` 时 arena `append` 可能触发 realloc，所以只能 copy。但每次 `equals/compare/get` 都 copy 是浪费。

改进：
- **arena 改 `[][]byte` 两级**：一级 slice 不扩容（append 到末尾 chunk 满就开新一块），第二级就是稳定的 byte 数组，`sliceLocked` 直接返回底层切片引用，零拷贝。
- 或加版本号 + RCU，读路径免 copy。

收益：Append hot path 每次少一次分配（每次 Append 都会走一次 `equals`），累积百兆级分配压力消失。

**Trade-off**：
- 两级 arena 的读路径需要先根据 offset 定位到第几个 chunk（一次除法或位运算 + 一次切片索引），Append 每样本增加 ~5 ns，**远小于 copy 省下的分配开销**，净正收益。
- 写入时若跨 chunk 边界需要处理 append 分裂；实现时让每个 chunk 固定大小（例如 1 MiB）并确保单条 labels 不跨 chunk（超过就新开一块）最简单。
- RCU 方案复杂度高，先不考虑。

### 2. symbolTable 的 string header 开销（P2）
当前 intern 一个 name 会同时在 `idx` map 和 `list` slice 里存 string header（16 B × 2）。Prometheus labels 中常见 name（`__name__`、`instance`）高度重复，intern 的 key 和 value 应共享底层字节。

改进：`list` 改 arena + offset，map 的 key 用 `uint32` offset 比较。

### 3. `labelsID` 永不回收 → arena 单调膨胀（P1）
`labelCatalog` 当前是 append-only；`sweepDeadSeries` 只动 hashIdx/refTab，**arena 条目留着**。Kubernetes 等高 churn 场景下 arena 会单调增长。

改进：
- 实现 `rebuildArena()`：flush 时若"死条目 bytes / 总 bytes > 阈值"，扫一遍 refTab 重建紧凑 arena + symbolTable，并更新每条 series 的 `labelsID`。需要在 flushMtx 下全局暂停一次。
- 或 arena 分代（每 flush 一代，代号随 truncate 整代丢）。

**Trade-off**：
- ⚠️ rebuildArena 是**全局 stop-the-world**：40M series 逐条更新 labelsID，估 5~20s，期间 Append 全部阻塞。**不可接受**。
- 改造方案：
  1. 在 flush 期间（反正写路径已经切到下一 window，对当前 labelCatalog 只读）**并行构建新 arena**，构建完成后原子切换 `h.labelCat`；
  2. 仍需原子更新每条 series 的 `labelsID`——让映射关系 `oldID -> newID` 保留一段时间，Append 查旧 ID 时懒更新即可；
  3. 阈值设得保守些（死条目占比 > 50% 才触发），不要每次 flush 都跑。
- 分代方案更简单：每个 flush window 一个独立 labelCatalog，window 关闭 + flush 完成后整代丢弃。实现代价低，**推荐先用这个**。

### 4. `get()` 分配完整 `labels.Labels`（P1）
blockReader 里同一个 labelsID 会在 `Postings` → `Series` → `LabelValues` 等方法里被反复 decode。

改进：提供按需 iterator（`forEachLabel(id, func(name, value string))`）走 decbuf，可完全零分配。

---

## 三、hashIndex

### 1. `buckets[i][hash] []refEntry` 分配链（P2）
每个 bucket 是 `map[uint64][]refEntry`，写入时 `append` 对单元素场景也会分配。

改进：
- 用 `map[uint64]refEntry` fast map + `overflow map[uint64][]refEntry`。99% 命中 fast map，无需 slice。
- 或把 `refEntry.labelsID` 去掉——可从 `refTab.get(ref).labelsID` 间接取。省 4 B × 总 series 数。

### 2. 双层锁开销（P2）
每次 Append 至少两次锁：hashIdx 分片锁 + labelCatalog 读锁。

改进：`refEntry` 里预存一个 `shortLabelsHash uint32`（或 xxhash 低位），链表先过这个短 hash，确认后再调一次 equals，减少 labelCatalog 锁命中。

---

## 四、refTable

### 1. `len()` O(n) 全扫（P3）
`series.go` 中 `refTable.len()` 两层循环。搜了下目前没有调用者；如果保留，改为维护 `numEntries atomic.Int64`。

### 2. `forEach` 长时间持读锁（P2）
sweep/flush 里用 `forEach` 全表扫，期间 `set()`/`del()` 全部阻塞。40M series 下写入延迟显著。

改进：按页粒度迭代，每处理一页放一次锁。对 GC/flush 而言弱一致可接受。

### 3. `[16384]*memSeries` 页空槽不回收（P3）
`del()` 只置 nil 不收缩页，长期高 churn 后大量半空页。

改进：flush 后统计每页活跃槽数，整页空置则释放。

---

## 五、appender / 写路径

### 1. appender pool 中大 slice "钉死"（P2）

```go
pendingSeries:  make([]record.RefSeries, 0, 256),
pendingSamples: make([]record.RefSample, 0, 1024),
sampleSeries:   make([]*memSeries, 0, 1024),
```

`reset()` 只 `[:0]`，处理过超大 batch 后 appender 会一直占着大数组。

改进：`reset()` 里若 `cap > threshold`（例如 4096）则丢弃重建。

### 2. `logWAL` 两次 `bufPool.Get`（P3）
一次 series 记录，一次 samples 记录。合并成一次 `enc.Encode` 输出更省。

### 3. `sealAndSpillLocked` 触发全局 `flushBlocking`（P1）
mmappedChunks 满触发"强制 flush"，走 `flushBlocking` 对**所有** series 做 block compaction + WAL truncate。单条 series 的局部问题触发全局停顿，40M series 下延迟极大。

改进：
- 允许 per-series 就地落盘（sealed chunk 已立即写出，本质只需腾空 `mmappedChunks` 数组），不触发 block compaction。
- 或增大 `maxMmappedChunksPerSeries` 上限（当前 8 偏小）。
- 或删除 forced flush 逻辑，改为覆盖式写入最老槽位 + 更新 `minValidTime`（会影响块边界，需要权衡）。

**Trade-off**：
- **最优解**：不要 per-series 触发全局 flush。将 `sealAndSpill` 改为：写完 sealed chunk 到 chunk 文件 → 立即 `mmappedChunks = mmappedChunks[1:]` 留出空位，**完全不调 flushBlocking**。当前 chunk 文件会增长但不影响正确性，真正的 block compaction 留给定时器按 window 边界触发。
- 这样单条 series 不会拖累全局，Append P99 从可能的数秒降到毫秒级。
- 风险：chunk 文件可能略大（不再严格限制总 mmappedChunks）；但真实写入路径是 Mimir ingester 自带限流，不会无限涨。

### 4. `chunkenc` pool 未长期复用（P2）
`chunkenc.NewPool()` 在 `compactHeadWindowOpts` 里建了一次又扔掉。`cutNewChunkLocked` 每次 `chunkenc.NewEmptyChunk` 分配。

改进：把 pool 放到 Head 层长期复用，`cutNewChunkLocked` 从 pool 取 chunk（参考标准 head）。

---

## 六、blockReader / flush 路径

### 1. `newBlockReader` 持锁遍历全部 series（P1）
`h.refTab.forEach` 每条都 `s.mu.Lock`，写路径抖动严重，40M series 下秒级延迟。

改进：
- 分批取快照（按 refPage），每批放锁让写路径继续。
- 或用 `seqlock` / `version` 字段，快照期只读，写路径 CAS 无阻塞。

**Trade-off**：
- 分批放锁会得到**弱一致快照**：快照开始到结束期间，部分 series 可能已产生新样本，但这些样本属于下一个 window（`minValidTime` 已切换），对本次 flush 没影响，**安全**。
- 每页处理时间 ~1ms，40M series 分 2500 页 → 总耗时 2~3s，但写路径抖动从秒级降到毫秒级。
- seqlock 方案 Append 多一次 atomic store（~2ns），CPU 影响可忽略。

### 2. `seriesSnapshot.chunks` 每次 flush 重新 `make`（P1）
40M series × 平均 2 chunks × 40 B/desc ≈ 3 GiB 瞬时分配，GC 压力大。

改进：pool 化 `[]chunkDescriptor`。

**Trade-off**：pure win，只是 flush 完要 `Put` 回 pool；GC 压力下降，Mark 阶段 CPU -5~10%。

### 3. `symbolSet` 全量重复 decode（P0，强烈推荐先做）
当前对每条 series 都走 `lc.get(labelsID)`（复制 arena + `NewScratchBuilder.Labels()`）仅为了收集 symbols。

改进：直接取 `labelCatalog.syms.list()` + sort —— arena append-only，symbolTable 恰好是 symbols 的超集。两三行代码，省下全量 decode。

**Trade-off**：
- 结果 symbols 集合是**超集**（包含已删除 series 的 symbols），这会让 block index 里的 symbols table 略大（KB 级），可完全接受。
- flush 期 CPU -30~50%（labels decode 是当前 flush 的主要开销之一），**纯正收益**。

### 4. Postings/LabelValues/... 的线性扫 + 全量 decode（P2）
compactor 实际只调 `AllPostings` + `Series`。确认 Mimir compactor 不调用其他方法后，其余接口降级为 panic 或返回空，不要每次都分配 map。

### 5. `openChunk` 快照 copy 全部字节（P0）
每条 series 的 open chunk 字节被 `copy(frozen, b)` 复制，40M × 1 KiB ≈ 40 GiB 瞬时峰值（flush 结束释放）。

改进：flush 期通过 `minValidTime` 已保证本 window open chunk 不会再被写（新样本走下一 window），字节稳定 —— 直接引用 `s.openChunk.Bytes()` 底层数组即可，只要确保 `sealAndSpillLocked` 不会 reuse 这块内存。保守起见可以保留 copy，但 buffer 从 pool 取。

**Trade-off**：
- **零拷贝方案**：需要严格保证 flush 期间 `openChunk` 的底层 buffer 不被写、不被归还 pool。快照完成前不能 `cutNewChunkLocked`（但 window 已切，本就不会）；`sealAndSpillLocked` 若复用 buffer 需改造。实现细节多但可行。
- **pool buffer 方案**：快照时从 `sync.Pool` 取 buf 做 copy，flush 结束归还。峰值降到稳态水平（几 GiB 复用）。**实现简单、风险低，先做这个**。
- CPU 影响：pool 方案和现状几乎一致；零拷贝方案 flush 期 CPU -20%。

---

## 七、其他

### 1. snapshot 单线程写（P2）
`writeSnapshot` 用 `refTab.forEach`（单线程），40M series encode 慢。按 refPage 并行编码，串行 `cp.Log`。

### 2. WAL replay 单线程（P2）
标准 head 支持 `WALReplayConcurrency`，litehead 虽然有该选项但 `loadWALSegments` 没真正并行。启动时间瓶颈。

### 3. Histogram/Exemplar/Metadata 静默成功（P3）
当前返回 `0, nil`。ingester 中被调到会静默丢数据。至少返回 `ErrUnsupported`，让上层明确知道。

### 4. `Meta()` 的 ULID 固定（P3）
当前用固定字符串 `"______lite______"`。多 block 去重逻辑会撞车。改为 `ulid.MustNew(ulid.Now(), rand)`。

---

## 八、优先级汇总（按"收益 × 改动成本 × CPU 风险"排序）

列说明：
- **内存收益**：稳态/瞬时节省
- **CPU 影响**：+ 表示 CPU 增加，- 表示减少，0 表示基本持平
- **延迟影响**：对 Append P99 或 flush 期抖动的影响

| 优先级 | 项 | 内存收益（40M series） | CPU 影响 | 延迟影响 | 改动成本 |
|---|---|---|---|---|---|
| P0 | `mmappedChunks [8]` 改 1+overflow | -7~10 GiB 常驻 | 0 | 0 | 小 |
| P0 | flush 时 `symbolSet` 直接取 `symbolTable.list` | -几百 MB 瞬时 | flush -30~50% | flush 抖动↓ | 很小 |
| P0 | open chunk snapshot 改 pool buf（先）/零拷贝（后） | -40 GiB 瞬时峰值 | 0 / flush -20% | flush 抖动↓ | 中 |
| P1 | labelCatalog arena 改 `[][]byte` | Append hot path -1 alloc/sample | Append +5ns | - | 中 |
| P1 | labelCatalog 死条目回收（分代 or 并行 rebuild） | 消除 arena 单调增长 | 0 | ⚠️ 必须并行/分代，否则 stall | 中 |
| P1 | `sealAndSpill` 去掉全局 forced flush | - | 0 | **Append P99 从秒级→毫秒级** | 中 |
| P1 | memSeries 字段重排 + 合并 `nextAt` | -16 B/series ≈ 600 MiB | Append -2~5%（cache 改善）| - | 小 |
| P1 | blockReader 分批锁 + `[]chunkDescriptor` pool | -3 GiB 瞬时 | flush -5~10% | flush 抖动↓ | 中 |
| P1 | labelCatalog `get()` 改 iterator 零分配 | flush 期 -上 GiB 瞬时 | flush -10% | - | 中 |
| P2 | hashIndex 单元素 fast map + refEntry 瘦身 | -100~200 MB | Append -1 alloc | - | 中 |
| P2 | `openApp` 从 series 移到 appender scratch | -600 MiB | ⚠️ 设计不当 CPU +30%；正确做法持平 | - | 中 |
| P2 | memSeries 锁外置/降级 | -300 MiB | ⚠️ 分片不足 CPU +10~20%；不推荐 | - | 中 |
| P2 | appender.reset 丢弃大 cap slice | 防内存"钉死" | 0 | - | 很小 |
| P2 | chunkenc pool 长期复用 | cut 新 chunk 零分配 | cut 路径 -少量 | - | 小 |
| P2 | WAL replay 并行 / snapshot 并行 | - | 启动期满载 CPU | 启动时间↓ | 中 |
| P2 | refTable.forEach 分批放锁 | - | 0 | 写抖动↓ | 小 |
| P2 | Postings 等接口降级 | flush 期分配 ↓ | flush -小 | - | 很小 |
| P3 | refTable.len O(1) 化 + 空页回收 | 小 | - | - | 小 |
| P3 | Histogram/Exemplar 返回 ErrUnsupported | - | - | 正确性 | 很小 |
| P3 | Meta().ULID 改真随机 | - | - | 正确性 | 很小 |
| P3 | `logWAL` 合并 bufPool.Get | 微量 | Append 微降 | - | 很小 |

**避坑点汇总**：

1. `memSeries` 锁降级（P2 中的一条）：盲目换分片锁可能 CPU +10~20%，不如保持 `sync.Mutex`，优先做字段重排。
2. `openApp` 移出 series（P2）：必须在 appender 层缓存 `chunkenc.Appender`，**不能**每次 Append 临时 `openChunk.Appender()`，否则 CPU +30~50%。
3. labelCatalog 死条目回收：严禁 stop-the-world rebuild；用分代方案或并行 rebuild + 懒 ID 迁移。
4. arena `[][]byte`：chunk 大小 ≥ 1 MiB，且保证单条 labels 不跨 chunk，Append 路径才简单。

---

## 九、建议先落地的三件事（收益大、风险低、CPU 零影响或负影响）

1. **flush 时 `symbolSet` 直接取 `symbolTable`**（`blockreader.go: newBlockReader`）
   - 代码量：3~5 行。
   - 收益：flush 期 CPU -30~50%，瞬时内存 -几百 MB。
   - 风险：symbols 是超集（无害）。
2. **`mmappedChunks` 改成"1 内联 + 溢出 slice"**
   - 涉及 `series.go` + `appender.sealAndSpillLocked` + `flush.truncateMmapped` + `blockreader.go` 遍历处。
   - 收益：-7 GiB 常驻。
   - 风险：CPU 0 影响（只有第 2 个 sealed chunk 时分配一次）。
3. **open chunk 快照改 pool buffer**（零拷贝先别做，先拿 80% 收益）
   - 涉及 `blockreader.go` snapshot 处。
   - 收益：-40 GiB 瞬时峰值。
   - 风险：实现简单，pool 命中后稳态几乎零分配。

这三件事组合起来：稳态 -7 GiB、flush 期 -40+ GiB 瞬时、flush CPU -30%，**全部对 Append hot path 零影响**。
