// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
)

// labelCatalog 把所有 series 的 labels 存成两级结构：
//
//  1. symbolTable：所有出现过的 label name / value 字符串，每个字符串只存一份，
//     分配一个递增的 symbol ID（uint32）。
//  2. arena：每条 series 的 labels 被编码成 [uvarint(nLabels) (uvarint(nameID)
//     uvarint(valueID))*nLabels]，不再直接写入字符串。
//
// 这样做的核心收益是去重：在时序场景里，label name 集合非常小（通常几十个），
// label value 也有大量重复（job、instance、namespace 等），在 40M 级别 series
// 下可以把 labels 的常驻内存显著压下来。
//
// 这里是 append-only 的：无论是 symbol table 还是 arena，都不会单独回收；
// 真正的回收只发生在 truncate/重建时。LiteHead 的稳态没有高频创建/删除
// series 的场景，这个代价可以接受。
//
// TODO(perf):
// 1. labelsID -> offset 用 []uint32 存；series churn 大时改成紧凑 map。
// 2. arena 扩容采用分段数组，避免单次 copy 成本过高。
// 3. symbol table 做 rebuild 时按使用频率重新排序，让热 symbol 的 ID 更小，
//    uvarint 编码更紧凑。
type labelCatalog struct {
	mu sync.RWMutex

	arena []byte
	// index[labelsID] = arena 中的起始 offset。
	index []uint32

	syms symbolTable
}

// labelCatalogInitialArenaCap 是 arena 的初始容量。按 40M series * 256B
// 估算，真实场景会远大于这个值，后续 append 扩容即可。
const labelCatalogInitialArenaCap = 1 << 20 // 1 MiB

func newLabelCatalog() *labelCatalog {
	lc := &labelCatalog{
		arena: make([]byte, 0, labelCatalogInitialArenaCap),
		index: make([]uint32, 0, 1024),
	}
	lc.syms.init()
	return lc
}

// put 把 lset 编码到 arena，返回分配到的 labelsID。
// 同一个 lset 多次 put 会产生不同的 labelsID；调用方需要保证唯一性
// （例如通过 hashIndex 做 dedup）。
func (lc *labelCatalog) put(lset labels.Labels) uint32 {
	// 先把所有 name/value 登记到 symbol table，拿到 ID 后再编码 arena。
	// 这里 symbol table 有自己的锁，和 arena 的锁错开，避免把字符串哈希
	// 的开销扛进 arena 的临界区。
	buf := encoding.Encbuf{}
	buf.PutUvarint(lset.Len())
	lset.Range(func(l labels.Label) {
		buf.PutUvarint32(lc.syms.intern(l.Name))
		buf.PutUvarint32(lc.syms.intern(l.Value))
	})

	lc.mu.Lock()
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
	return lc.decodeLabels(&dec)
}

// equals 比较 arena 中 id 对应的 labels 是否与 lset 等价。
// 只在 hashIndex 发生 hash 冲突时走到，属于冷路径。
// 这里按 symbol ID 解码后与 lset 中的字符串直接比较，省掉一次 Labels 分配。
func (lc *labelCatalog) equals(id uint32, lset labels.Labels) bool {
	lc.mu.RLock()
	offset := lc.index[id]
	var end uint32
	if int(id)+1 < len(lc.index) {
		end = lc.index[id+1]
	} else {
		end = uint32(len(lc.arena))
	}
	buf := make([]byte, end-offset)
	copy(buf, lc.arena[offset:end])
	lc.mu.RUnlock()

	dec := encoding.Decbuf{B: buf}
	n := dec.Uvarint()
	if n != lset.Len() {
		return false
	}

	equal := true
	i := 0
	lset.Range(func(l labels.Label) {
		if !equal {
			return
		}
		nameID := uint32(dec.Uvarint())
		valueID := uint32(dec.Uvarint())
		if lc.syms.lookup(nameID) != l.Name || lc.syms.lookup(valueID) != l.Value {
			equal = false
		}
		i++
	})
	return equal && dec.Err() == nil
}

// decodeLabels 按 symbol ID 序列还原 labels。
func (lc *labelCatalog) decodeLabels(dec *encoding.Decbuf) labels.Labels {
	b := labels.NewScratchBuilder(8)
	n := dec.Uvarint()
	for i := 0; i < n; i++ {
		nameID := uint32(dec.Uvarint())
		valueID := uint32(dec.Uvarint())
		b.Add(lc.syms.lookup(nameID), lc.syms.lookup(valueID))
	}
	return b.Labels()
}

// size 返回 arena 当前大小（字节）。监控用。
// 注意：这个值不包含 symbol table 自身的占用。
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

// symbolsSize 返回 symbol table 自身的大致字节占用。监控用。
func (lc *labelCatalog) symbolsSize() int {
	return lc.syms.bytes()
}

// symbolsCount 返回 symbol table 中不同字符串的个数。监控用。
func (lc *labelCatalog) symbolsCount() int {
	return lc.syms.count()
}

// ----- symbolTable -----
//
// symbolTable 是 append-only 的字符串池：同一个 label name / value 只存一份，
// 对外提供 string <-> uint32 ID 的互查。
//
// 结构上用 RWMutex + map[string]uint32 做去重，外加一个 []string 反查表。
// 在 LiteHead 的热路径里，绝大多数 label name/value 都已经存在，走 RLock
// 分支即可；只有首次出现的新字符串才需要升级到 WLock。

type symbolTable struct {
	mu   sync.RWMutex
	list []string          // ID -> string，ID 即下标
	idx  map[string]uint32 // string -> ID
}

func (s *symbolTable) init() {
	s.list = make([]string, 0, 1024)
	s.idx = make(map[string]uint32, 1024)
}

// intern 返回字符串对应的 symbol ID；不存在时插入。
// 走 RLock -> 双重检查 -> WLock 的模式，把热路径锁成本压到最低。
func (s *symbolTable) intern(str string) uint32 {
	s.mu.RLock()
	if id, ok := s.idx[str]; ok {
		s.mu.RUnlock()
		return id
	}
	s.mu.RUnlock()

	s.mu.Lock()
	// 双重检查：Lock 之前可能有别的 goroutine 先登记了同一个字符串。
	if id, ok := s.idx[str]; ok {
		s.mu.Unlock()
		return id
	}
	id := uint32(len(s.list))
	// 把 string 本体复制到 map key 上，避免上游 lset 里的切片被外部复用/修改。
	// Go 的 map 键是 string（immutable），隐式会做 header 拷贝，但底层字节仍是
	// 同一块内存；这里无需额外 strings.Clone：labels.Labels 的 Name/Value 在
	// 语义上是不可变字符串，由上游保证。
	s.list = append(s.list, str)
	s.idx[str] = id
	s.mu.Unlock()
	return id
}

// lookup 返回 ID 对应的字符串。ID 越界时返回空串（防御式；正常路径不会触发）。
func (s *symbolTable) lookup(id uint32) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if int(id) >= len(s.list) {
		return ""
	}
	return s.list[id]
}

// count 返回不同字符串的个数。
func (s *symbolTable) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.list)
}

// bytes 返回字符串原文占用的字节数（不含 map/slice 本身的额外开销）。
func (s *symbolTable) bytes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, v := range s.list {
		n += len(v)
	}
	return n
}
