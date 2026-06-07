package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// memSeries 保存一条 series 写入时的最小状态。
// labels 统一放在 labelCatalog，这里只存 labelsID。
//
// sealed chunks 采用 "1 inline + overflow slice" 表示：
// 绝大多数 series 在任一时刻只会持有 0~1 个 sealed mmapped chunk，
// 因此用内联单槽 + 按需 overflow 切片的组合可以显著降低稳态内存占用，
// 而不影响尾部持有大量 sealed chunk 的极端情况。
type memSeries struct {
	mu sync.Mutex

	ref      chunks.HeadSeriesRef
	labelsID uint32
	lastTs   int64

	openChunk chunkenc.Chunk
	openApp   chunkenc.Appender
	openMinT  int64
	openMaxT  int64
	nextAt    int64

	// batchGen 和 batchMaxT 配合 appender 的 batchGen 实现 O(1) 批内乱序检测。
	// 替代之前 appender 上的 map[*memSeries]int64，避免 map 分配和 delete 循环。
	// 每次 appender Commit/Rollback 递增 appender.batchGen，下次 Append 时
	// 如果 s.batchGen != a.batchGen 则说明上一批已经结束，可以直接覆盖。
	batchGen  uint64
	batchMaxT int64

	// inlineTs/inlineVal/inlineN 实现 open chunk 的懒初始化。
	// 对于低频 series（只写 1-2 个样本后就不再活跃的），延迟创建真正的
	// XOR chunk，节省 ~200-500B/series 的 chunk 分配开销。
	// 当 inlineN > 0 且 openChunk == nil 时，样本存储在 inline 中。
	// 一旦 inline 写满（inlineN == maxInlineSamples）或需要切 chunk 时，
	// 才真正创建 XOR chunk 并将 inline 样本回填进去。
	inlineTs  [maxInlineSamples]int64
	inlineVal [maxInlineSamples]float64
	inlineN   uint8

	// sealedCount 是 sealedInline + sealedOverflow 中活跃条目数。
	// sealedCount==0 时，sealedInline 无效；sealedCount==1 时，只有 sealedInline 有效；
	// sealedCount>=2 时，sealedInline 为第 0 条，sealedOverflow 为第 1..n-1 条。
	sealedCount    uint16
	sealedInline   mmappedChunk
	sealedOverflow []mmappedChunk
}

// maxInlineSamples 是 inline 样本缓冲区大小。设为 2 可以覆盖绝大多数
// "只写 1-2 个样本就不再活跃"的 churn 场景，同时保持 memSeries 结构紧凑。
// 增大此值会增加 memSeries 的基线大小（每个 slot 16 bytes），需要权衡。
const maxInlineSamples = 2

// hasInlineSamples 返回是否有尚未被 flush 到 open chunk 的 inline 样本。
// 调用方须持有 s.mu。
func (s *memSeries) hasInlineSamples() bool {
	return s.inlineN > 0 && s.openChunk == nil
}

// flushInlineToChunk 将 inline 样本回填到 open chunk 中。
// 调用方须持有 s.mu，且调用前已通过 ensureOpenChunk 创建了 chunk。
func (s *memSeries) flushInlineToChunk() {
	if s.inlineN == 0 || s.openChunk == nil {
		return
	}
	for i := uint8(0); i < s.inlineN; i++ {
		s.openApp.Append(s.inlineTs[i], s.inlineVal[i])
		if s.inlineTs[i] > s.openMaxT {
			s.openMaxT = s.inlineTs[i]
		}
	}
	s.inlineN = 0
}

// resetInline 清空 inline 样本（flush 后调用）。调用方须持有 s.mu。
func (s *memSeries) resetInline() {
	s.inlineN = 0
}

// defaultHardMmappedChunksPerSeries 是单条 series 在内存中允许持有的 sealed mmapped
// chunk 的**硬上限（hard limit）**的默认值。触达该值会触发一次 forced flush 作为
// 极端兜底保护；正常运行下这条路径不应该被走到。
//
// 对比历史：老版本使用 8 作为常量上限，与 sealed chunks 的 inline-array 表示耦合，
// 且 forced flush 在高 churn 场景会被频繁触发，把单条热点 series 的局部压力放大
// 成全局 stop-the-world。现在 sealed chunks 改成 inline + overflow 表示后，上限
// 不再受固定数组长度约束，默认值抬到 64——在典型 2h block / 15s scrape 的配置下，
// 任一 series 都不应该在一次 flush 间隔内攒到 64 个 sealed chunk，因此该值变成
// 事实上的"罕见兜底"。调用方可以通过 Options.ForcedFlushSealedChunks 覆盖。
const defaultHardMmappedChunksPerSeries = 64

// defaultSoftMmappedChunksPerSeries 是单条 series 的 **软告警阈值（soft watermark）**
// 的默认值。超过该值时，我们**不触发 forced flush**，只计数一次告警，让外部调用方
// 通过监控判断是否该把 Flush() 调用更激进。
//
// 该值的作用是把"何时 forced flush"这件事从代码里的硬编码，改成运行期可观测的
// 信号——一旦这条指标开始上升，就意味着外部 flush 节奏跟不上写入产生 sealed
// chunk 的节奏，需要调低 FlushCheckInterval 或检查 BlockDuration 配置。
const defaultSoftMmappedChunksPerSeries = 12

// maxMmappedChunksPerSeries 保留为包内常量，等于 hard 默认值，便于测试与其他
// 模块在缺省配置下参照。运行期实际生效的 hard/soft 阈值从 Head 读取。
const maxMmappedChunksPerSeries = defaultHardMmappedChunksPerSeries

// ----- sealed chunks 辅助方法 -----

// sealedLen 返回当前持有的 sealed mmapped chunk 数量。调用方须持有 s.mu。
func (s *memSeries) sealedLen() int {
	return int(s.sealedCount)
}

// sealedAt 返回第 i 条 sealed mmapped chunk 的指针。调用方须持有 s.mu。
// 越界时返回 nil。
func (s *memSeries) sealedAt(i int) *mmappedChunk {
	if i < 0 || i >= int(s.sealedCount) {
		return nil
	}
	if i == 0 {
		return &s.sealedInline
	}
	return &s.sealedOverflow[i-1]
}

// appendSealed 追加一条 sealed mmapped chunk。调用方须持有 s.mu。
func (s *memSeries) appendSealed(mc mmappedChunk) {
	if s.sealedCount == 0 {
		s.sealedInline = mc
		s.sealedCount = 1
		return
	}
	s.sealedOverflow = append(s.sealedOverflow, mc)
	s.sealedCount++
}

// retainSealedAfter 仅保留 maxTime > flushMaxt 的 sealed chunk，并返回被保留条目
// 覆盖的最小文件号（通过 keepMinFileNo 回调上报）。调用方须持有 s.mu。
//
// 使用回调上报 file number 可以避免在遍历时反复解 ref，也避免把 CDM 细节泄露到 series 内。
func (s *memSeries) retainSealedAfter(flushMaxt int64, onKeep func(mc mmappedChunk)) {
	if s.sealedCount == 0 {
		return
	}

	// 就地压缩：把要保留的条目挪到前段。
	// 语义与老数组的“就地压缩”等价。
	n := 0
	total := int(s.sealedCount)
	for i := 0; i < total; i++ {
		mc := *s.sealedAt(i)
		if mc.maxTime > flushMaxt {
			if onKeep != nil {
				onKeep(mc)
			}
			s.setSealedAt(n, mc)
			n++
		}
	}
	s.truncateSealed(n)
}

// forEachSealed 顺序遍历所有 sealed mmapped chunk。调用方须持有 s.mu。
func (s *memSeries) forEachSealed(fn func(mc mmappedChunk)) {
	if s.sealedCount == 0 {
		return
	}
	fn(s.sealedInline)
	for i := 0; i < len(s.sealedOverflow); i++ {
		fn(s.sealedOverflow[i])
	}
}

// setSealedAt 内部辅助：把第 i 条 sealed chunk 写为 mc。调用方须持有 s.mu。
// 仅用于 retainSealedAfter 的就地压缩路径。
func (s *memSeries) setSealedAt(i int, mc mmappedChunk) {
	if i == 0 {
		s.sealedInline = mc
		return
	}
	s.sealedOverflow[i-1] = mc
}

// truncateSealed 把 sealed chunk 数量缩减到 n（0 <= n <= sealedCount），
// 释放 overflow 中已无效的元素以消除遗留指针引用。调用方须持有 s.mu。
func (s *memSeries) truncateSealed(n int) {
	if n < 0 {
		n = 0
	}
	if n >= int(s.sealedCount) {
		return
	}
	if n == 0 {
		s.sealedInline = mmappedChunk{}
	}
	// 清空 overflow 中 n-1..len-1 的元素，避免潜在 aliasing。
	overflowKeep := 0
	if n > 1 {
		overflowKeep = n - 1
	}
	for i := overflowKeep; i < len(s.sealedOverflow); i++ {
		s.sealedOverflow[i] = mmappedChunk{}
	}
	s.sealedOverflow = s.sealedOverflow[:overflowKeep]
	s.sealedCount = uint16(n)
}

// mmappedChunk 是 sealed chunk 的最小引用信息。
type mmappedChunk struct {
	ref        chunks.ChunkDiskMapperRef
	minTime    int64
	maxTime    int64
	numSamples uint16
}

// ----- refTable：分页数组主索引 -----

const (
	refPageShift = 14
	refPageSize  = 1 << refPageShift
	refPageMask  = refPageSize - 1
)

type refPage struct {
	entries [refPageSize]*memSeries
}

// refTable 存储 ref -> *memSeries 的映射。
type refTable struct {
	mu    sync.RWMutex
	pages []*refPage
}

func newRefTable() *refTable { return &refTable{} }

func (t *refTable) get(ref chunks.HeadSeriesRef) *memSeries {
	pageIdx := uint64(ref) >> refPageShift
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
	return p.entries[uint64(ref)&refPageMask]
}

func (t *refTable) set(ref chunks.HeadSeriesRef, s *memSeries) {
	pageIdx := uint64(ref) >> refPageShift
	t.mu.Lock()
	for uint64(len(t.pages)) <= pageIdx {
		t.pages = append(t.pages, nil)
	}
	p := t.pages[pageIdx]
	if p == nil {
		p = &refPage{}
		t.pages[pageIdx] = p
	}
	p.entries[uint64(ref)&refPageMask] = s
	t.mu.Unlock()
}

// del 把 ref 对应的槽置空。
func (t *refTable) del(ref chunks.HeadSeriesRef) {
	pageIdx := uint64(ref) >> refPageShift
	t.mu.Lock()
	if pageIdx < uint64(len(t.pages)) {
		if p := t.pages[pageIdx]; p != nil {
			p.entries[uint64(ref)&refPageMask] = nil
		}
	}
	t.mu.Unlock()
}

// compactPages 释放所有条目为空的 page 指针（置 nil），并回收尾部连续 nil page
// 占用的 slice 空间。在 sweepDeadSeries 之后调用，可显著降低高 churn 场景下
// refTable 的长期内存占用。
//
// 成本：O(pages * pageSize)，每个 page 扫一遍判断是否全空。
// 在默认 pageSize=16384 下，100 万 series churn 产生 ~61 个 page，扫描耗时微秒级。
func (t *refTable) compactPages() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 把所有 entry 全空的 page 置 nil。
	for i, p := range t.pages {
		if p == nil {
			continue
		}
		empty := true
		for _, e := range p.entries {
			if e != nil {
				empty = false
				break
			}
		}
		if empty {
			t.pages[i] = nil
		}
	}

	// 从尾部裁剪连续 nil page，缩减 slice 长度。
	n := len(t.pages)
	for n > 0 && t.pages[n-1] == nil {
		n--
	}
	t.pages = t.pages[:n]
}

// forEach 遍历所有活跃 series。回调期间持有读锁，禁止在回调内写。
func (t *refTable) forEach(fn func(*memSeries)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.pages {
		if p == nil {
			continue
		}
		for _, e := range p.entries {
			if e != nil {
				fn(e)
			}
		}
	}
}

// snapshotPages 在读锁下返回 pages 的浅拷贝切片。返回的 `[]*refPage` 是新
// 的切片，但每个 `*refPage` 指针仍指向原结构。
//
// 设计意图：snapshot / flush 等批量遍历场景下，需要**短时间**持读锁获取 pages
// 列表，然后释放锁，**后续并行 worker 可独立读取各个 page**。这样避免在并行
// 编码阶段长期占用 `refTable.mu`，把写路径（新建 series）阻塞在上面。
//
// 并发正确性：
//   - `refPage.entries` 一旦被 `set` 赋值，槽位本身的指针可以被并发读（原子大小）；
//     `del` 可以把槽位置 nil，遍历侧需要判空，与现有 forEach 语义一致。
//   - 本方法仅用于写路径已停止或可容忍"读到稍旧视图"的场景（snapshot）。
func (t *refTable) snapshotPages() []*refPage {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*refPage, len(t.pages))
	copy(out, t.pages)
	return out
}

// ----- hashIndex：分片 flat map -----
//
// P1 优化：将每个 stripe 从 map[uint64][]refEntry 改为双层结构：
//   - primary: map[uint64]refEntry — 绝大部分 hash 只对应 1 条 series（无冲突），
//     直接存储 refEntry 而非 []refEntry 切片，省去 slice header (24B) + cap 对齐开销。
//   - overflow: map[uint64][]refEntry — 仅存储 hash 冲突的第 2..N 条 entry。
//
// 在典型生产场景中 >99% 的 hash 无冲突，overflow 极少被使用。
// 100 万 series 场景下预期节省 ~20-25 MB（每条 series 省去一个 slice header）。

const hashStripeCount = 256

type refEntry struct {
	ref      chunks.HeadSeriesRef
	labelsID uint32
}

type hashIndex struct {
	locks    [hashStripeCount]sync.RWMutex
	primary  [hashStripeCount]map[uint64]refEntry
	overflow [hashStripeCount]map[uint64][]refEntry
}

func newHashIndex() *hashIndex {
	h := &hashIndex{}
	for i := range h.primary {
		h.primary[i] = make(map[uint64]refEntry)
		h.overflow[i] = make(map[uint64][]refEntry)
	}
	return h
}

func (h *hashIndex) get(hash uint64, lset labels.Labels, lc *labelCatalog) (chunks.HeadSeriesRef, bool) {
	i := hash % hashStripeCount
	h.locks[i].RLock()
	// 先查 primary。
	if e, ok := h.primary[i][hash]; ok {
		if lc.equals(e.labelsID, lset) {
			h.locks[i].RUnlock()
			return e.ref, true
		}
		// primary 不匹配，查 overflow。
		for _, oe := range h.overflow[i][hash] {
			if lc.equals(oe.labelsID, lset) {
				h.locks[i].RUnlock()
				return oe.ref, true
			}
		}
	}
	h.locks[i].RUnlock()
	return 0, false
}

func (h *hashIndex) put(hash uint64, ref chunks.HeadSeriesRef, labelsID uint32) {
	i := hash % hashStripeCount
	e := refEntry{ref: ref, labelsID: labelsID}
	h.locks[i].Lock()
	if _, exists := h.primary[i][hash]; !exists {
		// 无冲突：直接存 primary。
		h.primary[i][hash] = e
	} else {
		// hash 冲突：追加到 overflow。
		h.overflow[i][hash] = append(h.overflow[i][hash], e)
	}
	h.locks[i].Unlock()
}

func (h *hashIndex) delete(hash uint64, ref chunks.HeadSeriesRef) {
	i := hash % hashStripeCount
	h.locks[i].Lock()
	pe, hasPrimary := h.primary[i][hash]
	if !hasPrimary {
		h.locks[i].Unlock()
		return
	}

	ov := h.overflow[i][hash]

	if pe.ref == ref {
		// 要删的是 primary entry。
		if len(ov) > 0 {
			// 把 overflow 的第一条提升为 primary。
			h.primary[i][hash] = ov[0]
			if len(ov) == 1 {
				delete(h.overflow[i], hash)
			} else {
				h.overflow[i][hash] = ov[1:]
			}
		} else {
			// 无 overflow，直接删除。
			delete(h.primary[i], hash)
		}
		h.locks[i].Unlock()
		return
	}

	// 要删的在 overflow 中。
	out := ov[:0]
	for _, e := range ov {
		if e.ref != ref {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		delete(h.overflow[i], hash)
	} else {
		h.overflow[i][hash] = out
	}
	h.locks[i].Unlock()
}
