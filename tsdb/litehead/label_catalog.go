// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
)

// labelCatalog 把所有 series 的 labels 放在一块连续的 arena 里，每条
// series 只保留一个 labelsID。arena 内部采用 WAL-Series 记录的同款格式，
// 可以直接借助 tsdb/encoding 做 uvarint 编解码。
//
// 这里是 append-only 的：labels 一旦写入 arena 就不会单独被回收，整段
// arena 的回收只发生在 truncate/重建时。LiteHead 的稳态没有高频
// 创建/删除 series 的场景，这个代价可以接受。
//
// TODO(perf):
// 1. labelsID -> offset 用 []uint32 存；series churn 大时改成紧凑 map。
// 2. arena 扩容采用分段数组，避免单次 copy 成本过高。
// 3. 做压缩/去重（symbol table）。
type labelCatalog struct {
	mu sync.RWMutex

	arena []byte
	// index[labelsID] = arena 中的起始 offset，长度以 arena 中 uvarint 计。
	index []uint32
}

// labelCatalogInitialArenaCap 是 arena 的初始容量。按 40M series * 256B
// 估算，真实场景会远大于这个值，后续 append 扩容即可。
const labelCatalogInitialArenaCap = 1 << 20 // 1 MiB

func newLabelCatalog() *labelCatalog {
	lc := &labelCatalog{
		arena: make([]byte, 0, labelCatalogInitialArenaCap),
		index: make([]uint32, 0, 1024),
	}
	return lc
}

// put 把 lset 编码到 arena，返回分配到的 labelsID。
// 同一个 lset 多次 put 会产生不同的 labelsID；调用方需要保证唯一性
// （例如通过 hashIndex 做 dedup）。
func (lc *labelCatalog) put(lset labels.Labels) uint32 {
	buf := encoding.Encbuf{}
	buf.PutUvarint(lset.Len())
	lset.Range(func(l labels.Label) {
		buf.PutUvarintStr(l.Name)
		buf.PutUvarintStr(l.Value)
	})

	lc.mu.Lock()
	// 如果 arena 变得过大，这里仍然是一次 append，不做单独的 GC。
	offset := uint32(len(lc.arena))
	lc.arena = append(lc.arena, buf.Get()...)
	id := uint32(len(lc.index))
	lc.index = append(lc.index, offset)
	lc.mu.Unlock()
	return id
}

// get 解码并返回指定 labelsID 的 labels。每次调用会分配一次新的 Labels。
// 性能敏感的调用方（例如 WAL 批量写）应当自行缓存结果。
func (lc *labelCatalog) get(id uint32) labels.Labels {
	lc.mu.RLock()
	offset := lc.index[id]
	// 找到下一条的 offset，作为 end；若是最后一条，end = len(arena)。
	var end uint32
	if int(id)+1 < len(lc.index) {
		end = lc.index[id+1]
	} else {
		end = uint32(len(lc.arena))
	}
	// 把切片拷出来（arena 内存复用是局部性的，但返回到外面可能随时被并发
	// append 迁移；这里做一次 copy 以免悬挂引用）。
	buf := make([]byte, end-offset)
	copy(buf, lc.arena[offset:end])
	lc.mu.RUnlock()

	dec := encoding.Decbuf{B: buf}
	return decodeLabels(&dec)
}

// equals 比较 arena 中 id 对应的 labels 是否与 lset 等价。仅用于冷路径
// hash 冲突校验；分配一次 Labels 再比较，简单可靠。
func (lc *labelCatalog) equals(id uint32, lset labels.Labels) bool {
	other := lc.get(id)
	return labels.Equal(other, lset)
}

// decodeLabels 按 WAL Series 的编码方式解码 labels。
func decodeLabels(dec *encoding.Decbuf) labels.Labels {
	b := labels.NewScratchBuilder(8)
	n := dec.Uvarint()
	for i := 0; i < n; i++ {
		name := dec.UvarintStr()
		value := dec.UvarintStr()
		b.Add(name, value)
	}
	return b.Labels()
}

// size 返回 arena 当前大小（字节）。监控用。
func (lc *labelCatalog) size() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.arena)
}

// count 返回已写入的 labels 数量。
func (lc *labelCatalog) count() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.index)
}
