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

	// sealedCount 是 sealedInline + sealedOverflow 中活跃条目数。
	// sealedCount==0 时，sealedInline 无效；sealedCount==1 时，只有 sealedInline 有效；
	// sealedCount>=2 时，sealedInline 为第 0 条，sealedOverflow 为第 1..n-1 条。
	sealedCount    uint16
	sealedInline   mmappedChunk
	sealedOverflow []mmappedChunk
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

// ----- hashIndex：分片 map -----

const hashStripeCount = 256

type refEntry struct {
	ref      chunks.HeadSeriesRef
	labelsID uint32
}

type hashIndex struct {
	locks   [hashStripeCount]sync.RWMutex
	buckets [hashStripeCount]map[uint64][]refEntry
}

func newHashIndex() *hashIndex {
	h := &hashIndex{}
	for i := range h.buckets {
		h.buckets[i] = make(map[uint64][]refEntry)
	}
	return h
}

func (h *hashIndex) get(hash uint64, lset labels.Labels, lc *labelCatalog) (chunks.HeadSeriesRef, bool) {
	i := hash % hashStripeCount
	h.locks[i].RLock()
	entries := h.buckets[i][hash]
	for _, e := range entries {
		if lc.equals(e.labelsID, lset) {
			h.locks[i].RUnlock()
			return e.ref, true
		}
	}
	h.locks[i].RUnlock()
	return 0, false
}

func (h *hashIndex) put(hash uint64, ref chunks.HeadSeriesRef, labelsID uint32) {
	i := hash % hashStripeCount
	h.locks[i].Lock()
	h.buckets[i][hash] = append(h.buckets[i][hash], refEntry{ref: ref, labelsID: labelsID})
	h.locks[i].Unlock()
}

func (h *hashIndex) delete(hash uint64, ref chunks.HeadSeriesRef) {
	i := hash % hashStripeCount
	h.locks[i].Lock()
	entries := h.buckets[i][hash]
	out := entries[:0]
	for _, e := range entries {
		if e.ref != ref {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		delete(h.buckets[i], hash)
	} else {
		h.buckets[i][hash] = out
	}
	h.locks[i].Unlock()
}
