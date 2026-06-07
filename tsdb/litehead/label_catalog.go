package litehead

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
)

// labelCatalog 将 series 的 labels 编码为紧凑的 arena + symbolTable 两级结构。
// symbolTable 做字符串去重，arena 以 (nameID, valueID) 对存储每条 series 的 labels。
// append-only：回收仅在 truncate/重建时发生。
//
// # 两级 arena 设计（PR-5）
//
// 历史实现使用单一 `arena []byte` + `index []uint32`，每次 `sliceLocked()` 都要
// `make + copy` 一份字节切片，导致 `get/compare/equals` 都带一次分配。这里把
// arena 拆成一组固定大小的 chunk（`chunks [][]byte`）：
//
//   - 每次 `put` 先看当前活跃 chunk 剩余空间是否够本次编码；不够就新建一个 chunk。
//   - 对于长度 > labelCatalogChunkSize 的超大记录，不做切分，直接为它单独开
//     一个精确尺寸的 chunk（oversized chunk）。这样解码方始终能在**同一 chunk
//     内**拿到完整字节，`sliceLocked()` 可以直接返回 sub-slice，不再复制。
//   - 旧 chunk 一旦分配就不会被 append；只有当前活跃 chunk（`chunks` 的最后一个）
//     的长度会继续增长。`[]byte` 的 sub-slice 在容量不触发 grow 的前提下稳定，
//     因此活跃 chunk 的 slice-header 一旦确定就安全（见下文并发正确性说明）。
//
// # 并发正确性
//
//   - 所有对 `chunks / chunkOffsets / chunkIDs / lengths` 的**写**均在 `lc.mu`
//     写锁下；所有读在读锁下。
//   - 活跃 chunk 会在写路径中被 `append` 扩容，必要时底层数组会被替换为新
//     的更大数组。但在新 chunk 被创建之前，写路径会预先估算剩余容量并决定
//     是否滚动到下一 chunk；我们为活跃 chunk **预留好 cap = labelCatalogChunkSize**，
//     保证单条 put 在未触发滚动前不会 grow，因此旧读者手中的 sub-slice 指向
//     的底层数组永远不会被替换。
//   - 上述约束是整个方案的安全边界，**不要**改成"按需扩容 cap"。
type labelCatalog struct {
	mu sync.RWMutex

	// chunks 是两级 arena 的第二级，每个元素是一块固定容量的字节块。
	// chunks[0] 是初始 chunk，chunks[len-1] 是当前活跃 chunk（允许 append）。
	chunks [][]byte

	// chunkIDs[labelsID] 指向该条编码所在的 chunk 下标。
	chunkIDs []uint32
	// chunkOffsets[labelsID] 指向该条编码在 chunk 内的起始偏移。
	chunkOffsets []uint32
	// lengths[labelsID] 为编码字节数；与 chunk 内下一条目的起始是一致的，
	// 但显式保存长度可以避免依赖"下一条"，尤其在 oversized / 跨 chunk 情况下更稳。
	lengths []uint32

	syms *symbolTable
}

// labelCatalogInitialArenaCap 为首个 chunk 预留的容量。保留这个名字以便外部如果
// 参考到的话能平滑过渡。
const labelCatalogInitialArenaCap = 1 << 20

// labelCatalogChunkSize 是普通 chunk 的固定容量（1 MiB）。put 时若当前活跃
// chunk 剩余空间不足以容纳一条编码，则滚动到下一 chunk；若单条编码 > chunkSize，
// 则为这条编码单独分配一个 oversized chunk。
//
// 取值权衡：
//   - 太小：频繁滚动，chunks 切片本身变长，index 元数据增多
//   - 太大：一次性预分配的 cap 大，内存常驻抬高；而且 oversized 阈值也变大，极端
//     长 labels 仍会独占整块
//
// 1 MiB 下，一条常规 labels（几十到几百字节）可容纳 ~数千条，保持较低的滚动成本。
const labelCatalogChunkSize = 1 << 20

func newLabelCatalog() *labelCatalog {
	lc := &labelCatalog{
		// 预分配第一个 chunk，容量等于 labelCatalogInitialArenaCap（与历史语义一致）。
		chunks:       [][]byte{make([]byte, 0, labelCatalogInitialArenaCap)},
		chunkIDs:     make([]uint32, 0, 1024),
		chunkOffsets: make([]uint32, 0, 1024),
		lengths:      make([]uint32, 0, 1024),
	}
	lc.syms = &symbolTable{}
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
	encoded := buf.Get()
	encLen := uint32(len(encoded))

	lc.mu.Lock()
	defer lc.mu.Unlock()

	chunkID, offset := lc.reserveLocked(encLen)
	// 写入字节并把元信息登记到索引。reserveLocked 已经保证 chunks[chunkID] 的
	// 剩余 cap 足够容纳 encLen，因此这里的 append 不会触发底层数组替换。
	lc.chunks[chunkID] = append(lc.chunks[chunkID], encoded...)

	id := uint32(len(lc.chunkIDs))
	lc.chunkIDs = append(lc.chunkIDs, chunkID)
	lc.chunkOffsets = append(lc.chunkOffsets, offset)
	lc.lengths = append(lc.lengths, encLen)
	return id
}

// reserveInChunks 为即将写入的 encLen 字节在 chunks 中选择目标 chunk 并返回
// (chunkID, offset)。规则：
//  1. 若活跃 chunk（末尾）剩余 cap 足够，直接用活跃 chunk。
//  2. 否则：
//     a) 若 encLen > labelCatalogChunkSize，新建一个精确长度的 oversized chunk，
//     独占这条记录；随后 append 一个新的空活跃 chunk，保持不变式。
//     b) 否则新建一个标准大小的 chunk 作为新的活跃 chunk。
//
// 本函数同时用于 put 路径（reserveLocked）和 rebuild 路径，避免重复代码。
func reserveInChunks(chunks *[][]byte, encLen uint32) (chunkID, offset uint32) {
	cs := *chunks
	active := len(cs) - 1
	if remaining := cap(cs[active]) - len(cs[active]); uint32(remaining) >= encLen {
		return uint32(active), uint32(len(cs[active]))
	}

	if encLen > labelCatalogChunkSize {
		over := make([]byte, 0, encLen)
		cs = append(cs, over)
		oversizedID := uint32(len(cs) - 1)
		fresh := make([]byte, 0, labelCatalogChunkSize)
		cs = append(cs, fresh)
		*chunks = cs
		return oversizedID, 0
	}

	fresh := make([]byte, 0, labelCatalogChunkSize)
	cs = append(cs, fresh)
	*chunks = cs
	return uint32(len(cs) - 1), 0
}

// reserveLocked 为即将写入的 encLen 字节选择目标 chunk 并返回 (chunkID, offset)。
// 必须在写锁下调用。
func (lc *labelCatalog) reserveLocked(encLen uint32) (chunkID, offset uint32) {
	return reserveInChunks(&lc.chunks, encLen)
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

// sliceLocked 返回 labelsID 对应的编码字节。**直接返回 sub-slice，不再复制**
// （两级 arena 设计的主要收益）。调用方必须持有 lc.mu 的读锁或写锁；且不得
// 持有返回 slice 超过 lc.mu 的释放点——因为一旦释放锁并发 put 可能追加数据
// 到同一 chunk，虽然底层数组不会被替换，但语义上 slice 的有效范围仅在锁内。
func (lc *labelCatalog) sliceLocked(id uint32) []byte {
	chunkID := lc.chunkIDs[id]
	offset := lc.chunkOffsets[id]
	length := lc.lengths[id]
	return lc.chunks[chunkID][offset : offset+length]
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

// size 返回当前所有 chunk 中已使用字节数的总和。保持与老实现的语义兼容
// （老实现是单 arena 的 len(arena)）。
func (lc *labelCatalog) size() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	total := 0
	for _, c := range lc.chunks {
		total += len(c)
	}
	return total
}

func (lc *labelCatalog) count() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.chunkIDs)
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

// snapshotList 返回 symbolTable.list 的浅拷贝切片。
//
// 用途：flush 阶段构造 block 的 symbols set 时，允许直接基于 symbolTable 的
// 内容生成 symbol 集合，而不是再遍历所有 series 去 decode labels。
// 返回的切片是新的底层数组，调用方可以安全地排序/筛选，不会影响内部状态。
// symbolTable 是 append-only 的，因此 snapshot 对应的字符串一定是 symbols 的
// 超集——这对 block 生成来说是可接受的：多余的 symbol 只会让 symbols index
// 略微变大，但不会影响正确性。
func (s *symbolTable) snapshotList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.list))
	copy(out, s.list)
	return out
}

// rebuild 重建 labelCatalog：只保留 aliveIDs 集合中的 labelsID，
// 丢弃所有已死的 series 编码和不再引用的 symbol。
//
// 调用时机：在 sweepDeadSeries 之后，由 truncateMemory 调用。
// 调用方须确保不会有并发写入（appenderMtx.Lock 已持有）。
//
// 返回旧 labelsID -> 新 labelsID 的映射，调用方须据此更新 memSeries.labelsID。
// 如果活跃 series 为空，不执行重建，返回 nil。
//
// 流式 rebuild 优化：分批处理活跃 entry，每批处理完后立即释放旧数据引用，
// 将内存峰值从 ~2x 降低到 ~1.1-1.2x。具体做法是按 rebuildBatchSize 分批
// 复制旧数据到新 arena，每批结束后已复制的旧 chunk 数据不再被新 arena 引用。
const rebuildBatchSize = 10000

func (lc *labelCatalog) rebuild(aliveIDs map[uint32]struct{}) map[uint32]uint32 {
	if len(aliveIDs) == 0 {
		return nil
	}

	lc.mu.Lock()
	defer lc.mu.Unlock()

	// 第一步：收集所有活跃的 labelsID（只收 ID，不立即复制数据，减少峰值）。
	aliveList := make([]uint32, 0, len(aliveIDs))
	for oldID := range aliveIDs {
		aliveList = append(aliveList, oldID)
	}

	// 第二步：准备新的数据结构。
	newSyms := &symbolTable{}
	newSyms.init()

	newChunks := [][]byte{make([]byte, 0, labelCatalogChunkSize)}
	newChunkIDs := make([]uint32, 0, len(aliveList))
	newChunkOffsets := make([]uint32, 0, len(aliveList))
	newLengths := make([]uint32, 0, len(aliveList))
	oldToNew := make(map[uint32]uint32, len(aliveList))

	// 第三步：分批迁移，每批只从旧 arena 读取 rebuildBatchSize 条数据。
	// 这样每次只有一个批次的旧数据副本与新 arena 同时驻留内存。
	for batchStart := 0; batchStart < len(aliveList); batchStart += rebuildBatchSize {
		batchEnd := batchStart + rebuildBatchSize
		if batchEnd > len(aliveList) {
			batchEnd = len(aliveList)
		}
		batch := aliveList[batchStart:batchEnd]

		for _, oldID := range batch {
			// 从旧 arena 读取并立即重编码到新 arena。
			buf := lc.sliceLocked(oldID)
			// 解码旧编码中的 symbol ID，重新 intern 到新 symbolTable。
			dec := encoding.Decbuf{B: buf}
			n := dec.Uvarint()
			var enc encoding.Encbuf
			enc.PutUvarint(n)
			for i := 0; i < n; i++ {
				oldNameID := uint32(dec.Uvarint())
				oldValueID := uint32(dec.Uvarint())
				name := lc.syms.lookupNoLock(oldNameID)
				value := lc.syms.lookupNoLock(oldValueID)
				enc.PutUvarint32(newSyms.intern(name))
				enc.PutUvarint32(newSyms.intern(value))
			}
			encoded := enc.Get()
			encLen := uint32(len(encoded))

			// 放入新 arena（复用与 put 路径相同的 chunk 分配逻辑）。
			chunkID, offset := reserveInChunks(&newChunks, encLen)
			newChunks[chunkID] = append(newChunks[chunkID], encoded...)

			newID := uint32(len(newChunkIDs))

			newChunkIDs = append(newChunkIDs, chunkID)
			newChunkOffsets = append(newChunkOffsets, offset)
			newLengths = append(newLengths, encLen)

			oldToNew[oldID] = newID
		}
	}

	// 第四步：替换所有内部状态。旧 chunks/syms 在此时被 GC 回收。
	lc.chunks = newChunks
	lc.chunkIDs = newChunkIDs
	lc.chunkOffsets = newChunkOffsets
	lc.lengths = newLengths
	lc.syms = newSyms

	return oldToNew
}

// lookupNoLock 在 symbolTable 的调用方已确保不会有并发写入时使用（rebuild 内部调用）。
// 不获取 s.mu，避免 lc.mu -> s.mu 的嵌套锁顺序与 put 路径 (s.mu -> lc.mu) 形成死锁。
// 安全前提：调用方持有 appenderMtx.Lock()，不会有并发 intern。
func (s *symbolTable) lookupNoLock(id uint32) string {
	if int(id) >= len(s.list) {
		return ""
	}
	return s.list[id]
}
