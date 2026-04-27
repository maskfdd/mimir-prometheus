package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
)

// labelCatalog 将 series 的 labels 编码为紧凑的 arena + symbolTable 两级结构。
// symbolTable 做字符串去重，arena 以 (nameID, valueID) 对存储每条 series 的 labels。
// append-only：回收仅在 truncate/重建时发生。
type labelCatalog struct {
	mu sync.RWMutex

	arena []byte
	// index[labelsID] = arena 中的起始 offset。
	index []uint32

	syms symbolTable
}

const labelCatalogInitialArenaCap = 1 << 20

func newLabelCatalog() *labelCatalog {
	lc := &labelCatalog{
		arena: make([]byte, 0, labelCatalogInitialArenaCap),
		index: make([]uint32, 0, 1024),
	}
	lc.syms.init()
	return lc
}

// put 编码 lset 到 arena，返回 labelsID。
func (lc *labelCatalog) put(lset labels.Labels) uint32 {
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

// get 解码并返回 labelsID 对应的 labels。
func (lc *labelCatalog) get(id uint32) labels.Labels {
	lc.mu.RLock()
	buf := lc.sliceLocked(id)
	lc.mu.RUnlock()

	dec := encoding.Decbuf{B: buf}
	return lc.decodeLabels(&dec)
}

// compare 在 arena 编码上比较两个 labelsID 的 labels 顺序，不分配 Labels。
func (lc *labelCatalog) compare(a, b uint32) int {
	lc.mu.RLock()
	bufA := lc.sliceLocked(a)
	bufB := lc.sliceLocked(b)
	lc.mu.RUnlock()

	decA := encoding.Decbuf{B: bufA}
	decB := encoding.Decbuf{B: bufB}
	nA := decA.Uvarint()
	nB := decB.Uvarint()

	n := nA
	if nB < n {
		n = nB
	}
	for i := 0; i < n; i++ {
		nameA := lc.syms.lookup(uint32(decA.Uvarint()))
		nameB := lc.syms.lookup(uint32(decB.Uvarint()))
		if nameA < nameB {
			return -1
		}
		if nameA > nameB {
			return 1
		}
		valA := lc.syms.lookup(uint32(decA.Uvarint()))
		valB := lc.syms.lookup(uint32(decB.Uvarint()))
		if valA < valB {
			return -1
		}
		if valA > valB {
			return 1
		}
	}
	// 所有共同的 label 都相同；按 label 数量区分。
	if nA < nB {
		return -1
	}
	if nA > nB {
		return 1
	}
	return 0
}

func (lc *labelCatalog) sliceLocked(id uint32) []byte {
	offset := lc.index[id]
	var end uint32
	if int(id)+1 < len(lc.index) {
		end = lc.index[id+1]
	} else {
		end = uint32(len(lc.arena))
	}
	buf := make([]byte, end-offset)
	copy(buf, lc.arena[offset:end])
	return buf
}

func (lc *labelCatalog) equals(id uint32, lset labels.Labels) bool {
	lc.mu.RLock()
	buf := lc.sliceLocked(id)
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

func (lc *labelCatalog) size() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.arena)
}

func (lc *labelCatalog) count() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.index)
}

func (lc *labelCatalog) symbolsSize() int {
	return lc.syms.bytes()
}

func (lc *labelCatalog) symbolsCount() int {
	return lc.syms.count()
}

// ----- symbolTable -----
//
// append-only 的字符串池，提供 string <-> uint32 ID 互查。

type symbolTable struct {
	mu   sync.RWMutex
	list []string          // ID -> string，ID 即下标
	idx  map[string]uint32 // string -> ID
}

func (s *symbolTable) init() {
	s.list = make([]string, 0, 1024)
	s.idx = make(map[string]uint32, 1024)
}

func (s *symbolTable) intern(str string) uint32 {
	s.mu.RLock()
	if id, ok := s.idx[str]; ok {
		s.mu.RUnlock()
		return id
	}
	s.mu.RUnlock()

	s.mu.Lock()
	if id, ok := s.idx[str]; ok {
		s.mu.Unlock()
		return id
	}
	id := uint32(len(s.list))
	s.list = append(s.list, str)
	s.idx[str] = id
	s.mu.Unlock()
	return id
}

func (s *symbolTable) lookup(id uint32) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if int(id) >= len(s.list) {
		return ""
	}
	return s.list[id]
}

func (s *symbolTable) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.list)
}

func (s *symbolTable) bytes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, v := range s.list {
		n += len(v)
	}
	return n
}
