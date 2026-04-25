// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// memSeries 保存一条 series 写入时需要的最小状态。命名与标准
// tsdb.Head 的 memSeries 对齐，方便与原生 Head 互相对照阅读。
//
// 注意：这里不保存 labels。labels 统一放在 labelCatalog 里，memSeries
// 只通过 labelsID 索引回 labels。这样做是为了让热路径下每条 series 的
// 常驻字段尽量少。
type memSeries struct {
	// mu 保护 open chunk 的追加与 mmappedChunks 的更新。
	mu sync.Mutex

	ref      chunks.HeadSeriesRef
	labelsID uint32

	// 最近一条样本的时间戳。乱序样本判定时使用。
	lastTs int64

	// open chunk 相关状态。open chunk 按需懒分配，没有样本时为 nil。
	openChunk chunkenc.Chunk
	openApp   chunkenc.Appender
	openMinT  int64
	openMaxT  int64
	// nextAt 是当前 chunk 的“最晚结束时间”上界（按 chunkRange 对齐）。
	nextAt int64

	// mmappedChunks 定长数组，保存窗口内已经 spill 到磁盘的 chunk 元数据。
	// 命名与标准 tsdb.memSeries.mmappedChunks 对齐。
	// 超过容量时将触发一次小型本 series 的强制 flush/seal 合并。
	mmappedChunksCount uint8
	mmappedChunks      [maxMmappedChunksPerSeries]mmappedChunk
}

// maxMmappedChunksPerSeries 限制每条 series 在 flush 之前可持有的
// mmapped chunk 数量。窗口期内通常只会有 1~2 个。
const maxMmappedChunksPerSeries = 8

// mmappedChunk 是一条 series 的 sealed chunk 的最小引用。
// 真正的字节已经通过 ChunkDiskMapper 落到磁盘上。命名与标准
// tsdb.mmappedChunk 对齐。
type mmappedChunk struct {
	ref      chunks.ChunkDiskMapperRef
	minTime  int64
	maxTime  int64
	encoding chunkenc.Encoding
	// 样本数，仅用于 flush 阶段做估算/监控。
	numSamples uint16
}

// ----- refTable：分页数组主索引 -----
//
// 因为 ref 是自增整数（从 1 开始），把它作为“分页数组”的下标使用，
// 可以省掉 map 的 bucket 和指针开销。每页是一个 [pageSize]*writeSeries
// 的定长数组，页级按需分配。
//
// 注意：这里不做 ref 的 GC。稳态下 series 是长期存在的；需要 GC 时应交给
// truncate 流程整体重建。

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

// del 把 ref 对应的槽置空。仅在 flush 后 GC 路径里使用。
// 调用方负责保证此时没有 appender 正在持有这个 memSeries 的 mu。
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

// forEach 遍历所有活跃 series，回调里可以读 series 字段；
// 回调期间持有 refTable 读锁，禁止在回调里再获取写锁（比如再调用 del）。
// 如果需要在遍历中决定删除，先把 ref 收集到 slice 里，遍历结束后再 del。
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

// ----- hashIndex：分片冷路径 map -----
//
// 热路径优先走 ref，只有在 appender 没有 ref 或者 ref 失效时才走 hashIndex
// 做 labels -> ref 的查找。为了减少常驻内存，槽内不直接保存 labels，而是
// 保存 labelsID，发生 hash 冲突时再回 labelCatalog 取 labels 做比较。

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

// get 在 hash 槽里查找 labels 对应的 ref。lc 用于解码 labels 做精确比较。
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

// delete 从 hash 槽里删除一个 ref。truncate/GC 时使用。
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
