package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// memSeries 保存一条 series 写入时的最小状态。
// labels 统一放在 labelCatalog，这里只存 labelsID。
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

	mmappedChunksCount uint8
	mmappedChunks      [maxMmappedChunksPerSeries]mmappedChunk
}

const maxMmappedChunksPerSeries = 8

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

// len 返回当前已注册的 series 数量（近似值，仅用于 metrics）。
func (t *refTable) len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	n := 0
	for _, p := range t.pages {
		if p == nil {
			continue
		}
		for _, e := range p.entries {
			if e != nil {
				n++
			}
		}
	}
	return n
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
