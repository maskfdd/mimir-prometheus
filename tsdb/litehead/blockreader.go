package litehead

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// blockReader 把 litehead.Head 包装成 tsdb.BlockReader，
// 直接喂给 LeveledCompactor.Write。
//
// 快照在 newBlockReader 时一次性采集，后续读操作不再回主索引。
//
// 生命周期：Index() 和 Chunks() 各自返回的 reader 都持有对 blockReader 的
// 反向引用；任一 reader Close 时会递减 openReaders 计数，计数归零后
// 把复用的 open-chunk buffer 归还到 Head 的 snapshotBufPool。
type blockReader struct {
	head       *Head
	mint, maxt int64

	meta tsdb.BlockMeta

	series    []*seriesSnapshot // 按 labels 排序
	symbolSet []string

	refIndex []refIndexEntry // 按 ref 排序，用于二分查找

	// openBufs 保存本次快照从 snapshotBufPool 借来的 *[]byte。
	// 所有 reader 全部 Close 后才会被归还，避免在 compactor 还在读
	// openBytes 的时候提前回收导致 use-after-free。
	openBufs []*[]byte
	// openReaders 记录尚未 Close 的 IndexReader + ChunkReader 数量。
	// Index() 和 Chunks() 各 +1，对应的 Close() 各 -1；归零时归还 openBufs。
	openReaders atomic.Int32
	released    atomic.Bool
}

// refIndexEntry 存储 series ref 到 series 切片下标的映射。
type refIndexEntry struct {
	ref chunks.HeadSeriesRef
	idx int
}

// seriesSnapshot 是 series 在 flush 时的只读投影。
type seriesSnapshot struct {
	ref      chunks.HeadSeriesRef
	labelsID uint32
	chunks   []chunkDescriptor
}

type chunkDescriptor struct {
	minTime int64
	maxTime int64

	kind         chunkSource
	mmappedRef   chunks.ChunkDiskMapperRef
	openEncoding chunkenc.Encoding
	openBytes    []byte
}

type chunkSource uint8

const (
	chunkSourceMmapped chunkSource = iota
	chunkSourceOpen
)

// newBlockReader 采集 series 快照。
//
// 注意 openReaders 的初始值是 1，对应 blockReader 自身持有的一次引用。
// 调用方（flush 路径）在 compactor.Write 返回后必须调用 br.done() 释放这次引用，
// 否则从 snapshotBufPool 借来的 buffer 不会被归还。
func newBlockReader(h *Head, mint, maxt int64) *blockReader {
	r := &blockReader{
		head: h,
		mint: mint,
		maxt: maxt,
	}
	r.openReaders.Store(1)

	h.refTab.forEach(func(s *memSeries) {
		s.mu.Lock()
		if !seriesOverlapsWindowLocked(s, mint, maxt) {
			s.mu.Unlock()
			return
		}
		descs := make([]chunkDescriptor, 0, s.sealedLen()+1)
		for i := 0; i < s.sealedLen(); i++ {
			mc := *s.sealedAt(i)
			if mc.maxTime < mint || mc.minTime > maxt {
				continue
			}
			descs = append(descs, chunkDescriptor{
				minTime:    mc.minTime,
				maxTime:    mc.maxTime,
				kind:       chunkSourceMmapped,
				mmappedRef: mc.ref,
			})
		}
		if s.openChunk != nil && s.openChunk.NumSamples() > 0 {
			openMinT := s.openMinT
			openMaxT := s.openMaxT
			if openMaxT >= mint && openMinT <= maxt {
				b := s.openChunk.Bytes()
				// 从 snapshotBufPool 借出 scratch buffer 来冻结 open chunk 字节。
				// pool 对象是 *[]byte；我们按需扩容，再把 live bytes copy 进去。
				// 对应的 *[]byte 统一挂到 r.openBufs，由 reader 全部 Close 后一次性归还。
				pb := h.snapshotBufPool.Get().(*[]byte)
				buf := (*pb)
				if cap(buf) < len(b) {
					buf = make([]byte, len(b))
				} else {
					buf = buf[:len(b)]
				}
				copy(buf, b)
				*pb = buf
				r.openBufs = append(r.openBufs, pb)
				// 对齐标准 Head 的对外语义：仍把 open chunk 暴露成"可增长"块。
				// 真正的字节和编码已经在快照阶段冻结，避免 Chunk() 再回看 live 状态。
				descs = append(descs, chunkDescriptor{
					minTime:      openMinT,
					maxTime:      math.MaxInt64,
					kind:         chunkSourceOpen,
					openEncoding: s.openChunk.Encoding(),
					openBytes:    buf,
				})
			}
		}
		if len(descs) == 0 {
			s.mu.Unlock()
			return
		}
		snap := &seriesSnapshot{
			ref:      s.ref,
			labelsID: s.labelsID,
			chunks:   descs,
		}
		s.mu.Unlock()

		r.series = append(r.series, snap)
	})

	// 按 labels 排序。
	slices.SortFunc(r.series, func(a, b *seriesSnapshot) int {
		return h.labelCat.compare(a.labelsID, b.labelsID)
	})

	// 构建 refIndex。
	r.refIndex = make([]refIndexEntry, len(r.series))
	for i, snap := range r.series {
		r.refIndex[i] = refIndexEntry{ref: snap.ref, idx: i}
	}
	sort.Slice(r.refIndex, func(i, j int) bool {
		return r.refIndex[i].ref < r.refIndex[j].ref
	})

	// 收集 symbols。
	//
	// 这里直接基于 symbolTable 的内容快照构造 symbolSet，
	// 而不是再遍历所有 series 去 decode labels：
	//   - symbolTable 是 append-only 的，所以它一定是本次 flush 所需 symbols 的超集；
	//   - block 的 symbols index 允许出现未被任何 series 引用的符号——compactor
	//     只按顺序 emit symbols，不做引用计数；
	//   - 这样可以在 flush 路径上消除一次全量 labels decode，flush CPU 和
	//     瞬时内存都会明显下降。
	r.symbolSet = h.labelCat.syms.snapshotList()
	slices.Sort(r.symbolSet)

	r.meta = tsdb.BlockMeta{
		ULID:    ulid.ULID{},
		MinTime: mint,
		MaxTime: maxt,
	}
	return r
}

// lookupRef 二分查找 ref 在 series 切片中的下标。
func (r *blockReader) lookupRef(ref storage.SeriesRef) (int, bool) {
	sref := chunks.HeadSeriesRef(ref)
	i := sort.Search(len(r.refIndex), func(i int) bool {
		return r.refIndex[i].ref >= sref
	})
	if i < len(r.refIndex) && r.refIndex[i].ref == sref {
		return r.refIndex[i].idx, true
	}
	return 0, false
}

// seriesOverlapsWindowLocked 判断 series 在 [mint, maxt] 范围内有没有可读样本。
// 调用方必须持有 s.mu。
func seriesOverlapsWindowLocked(s *memSeries, mint, maxt int64) bool {
	for i := 0; i < s.sealedLen(); i++ {
		mc := *s.sealedAt(i)
		if mc.maxTime >= mint && mc.minTime <= maxt {
			return true
		}
	}
	if s.openChunk != nil && s.openChunk.NumSamples() > 0 {
		if s.openMaxT >= mint && s.openMinT <= maxt {
			return true
		}
	}
	return false
}

// ---------------- BlockReader ----------------

func (r *blockReader) Meta() tsdb.BlockMeta { return r.meta }

func (r *blockReader) Size() int64 { return 0 }

func (r *blockReader) Index() (tsdb.IndexReader, error) {
	r.openReaders.Add(1)
	return &indexReader{r: r}, nil
}

func (r *blockReader) Chunks() (tsdb.ChunkReader, error) {
	r.openReaders.Add(1)
	return &chunkReader{r: r}, nil
}

func (r *blockReader) Tombstones() (tombstones.Reader, error) {
	return tombstones.NewMemTombstones(), nil
}

// release 递减 openReaders 计数；计数归零后把 openBufs 归还到 Head 的 snapshotBufPool。
//
// 这里只 reset slice 长度（保留底层 cap），让 pool 里的下一次请求可以
// 直接复用已分配的 buffer 容量，避免反复 grow。
func (r *blockReader) release() {
	if r.openReaders.Add(-1) > 0 {
		return
	}
	// CAS 保证即使出现多次重复 Close 也只归还一次。
	if !r.released.CompareAndSwap(false, true) {
		return
	}
	for _, pb := range r.openBufs {
		if pb == nil {
			continue
		}
		*pb = (*pb)[:0]
		r.head.snapshotBufPool.Put(pb)
	}
	r.openBufs = nil
}

// done 释放 newBlockReader 初始化时预留的那一次引用。
// flush 路径在 compactor.Write 返回后必须调用一次，无论成功与否，
// 以确保 snapshotBufPool 借出的 buffer 最终一定被归还。
func (r *blockReader) done() {
	r.release()
}

// ---------------- IndexReader ----------------

type indexReader struct {
	r *blockReader
}

func (ir *indexReader) Close() error {
	ir.r.release()
	return nil
}

func (ir *indexReader) Symbols() index.StringIter {
	return index.NewStringListIter(ir.r.symbolSet)
}

// Postings 返回匹配 name/values 的 series refs。
func (ir *indexReader) Postings(_ context.Context, name string, values ...string) (index.Postings, error) {
	if len(values) == 0 {
		return index.EmptyPostings(), nil
	}

	// AllPostingsKey 快速路径：name == "" && values == [""]
	if name == "" && len(values) == 1 && values[0] == "" {
		refs := make([]storage.SeriesRef, len(ir.r.series))
		for i, s := range ir.r.series {
			refs[i] = storage.SeriesRef(s.ref)
		}
		return index.NewListPostings(refs), nil
	}

	// 通用 fallback：线性扫描匹配。
	valSet := make(map[string]struct{}, len(values))
	for _, v := range values {
		valSet[v] = struct{}{}
	}
	refs := make([]storage.SeriesRef, 0, len(ir.r.series))
	for _, s := range ir.r.series {
		lset := ir.r.head.labelCat.get(s.labelsID)
		if _, ok := valSet[lset.Get(name)]; ok {
			refs = append(refs, storage.SeriesRef(s.ref))
		}
	}
	return index.NewListPostings(refs), nil
}

// SortedPostings：series 已按 labels 排序，直接 pass-through。
func (ir *indexReader) SortedPostings(p index.Postings) index.Postings {
	return p
}

func (ir *indexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	var out []storage.SeriesRef
	for p.Next() {
		ref := p.At()
		idx, ok := ir.r.lookupRef(ref)
		if !ok {
			continue
		}
		lset := ir.r.head.labelCat.get(ir.r.series[idx].labelsID)
		if labels.StableHash(lset)%shardCount != shardIndex {
			continue
		}
		out = append(out, ref)
	}
	return index.NewListPostings(out)
}

func (ir *indexReader) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	idx, ok := ir.r.lookupRef(ref)
	if !ok {
		return storage.ErrNotFound
	}
	snap := ir.r.series[idx]

	lset := ir.r.head.labelCat.get(snap.labelsID)
	builder.Reset()
	lset.Range(func(l labels.Label) {
		builder.Add(l.Name, l.Value)
	})

	if chks == nil {
		return nil
	}
	*chks = (*chks)[:0]
	for i, d := range snap.chunks {
		*chks = append(*chks, chunks.Meta{
			MinTime: d.minTime,
			MaxTime: d.maxTime,
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(snap.ref, chunks.HeadChunkID(i))),
		})
	}
	return nil
}

// 以下方法 compactor 不会调用，提供朴素实现满足接口要求。

func (ir *indexReader) SortedLabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error) {
	v, err := ir.LabelValues(ctx, name, matchers...)
	if err != nil {
		return nil, err
	}
	slices.Sort(v)
	return v, nil
}

func (ir *indexReader) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error) {
	set := make(map[string]struct{}, 16)
	for _, s := range ir.r.series {
		lset := ir.r.head.labelCat.get(s.labelsID)
		if v := lset.Get(name); v != "" {
			set[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	return out, nil
}

func (ir *indexReader) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, error) {
	set := make(map[string]struct{}, 16)
	for _, s := range ir.r.series {
		lset := ir.r.head.labelCat.get(s.labelsID)
		lset.Range(func(l labels.Label) {
			set[l.Name] = struct{}{}
		})
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, nil
}

func (ir *indexReader) PostingsForMatchers(ctx context.Context, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	var refs []storage.SeriesRef
	for _, s := range ir.r.series {
		lset := ir.r.head.labelCat.get(s.labelsID)
		match := true
		for _, m := range ms {
			if !m.Matches(lset.Get(m.Name)) {
				match = false
				break
			}
		}
		if match {
			refs = append(refs, storage.SeriesRef(s.ref))
		}
	}
	return index.NewListPostings(refs), nil
}

func (ir *indexReader) LabelValueFor(_ context.Context, id storage.SeriesRef, label string) (string, error) {
	idx, ok := ir.r.lookupRef(id)
	if !ok {
		return "", storage.ErrNotFound
	}
	lset := ir.r.head.labelCat.get(ir.r.series[idx].labelsID)
	v := lset.Get(label)
	if v == "" {
		return "", storage.ErrNotFound
	}
	return v, nil
}

func (ir *indexReader) LabelNamesFor(ctx context.Context, ids ...storage.SeriesRef) ([]string, error) {
	set := make(map[string]struct{}, 16)
	for _, id := range ids {
		idx, ok := ir.r.lookupRef(id)
		if !ok {
			return nil, storage.ErrNotFound
		}
		lset := ir.r.head.labelCat.get(ir.r.series[idx].labelsID)
		lset.Range(func(l labels.Label) {
			set[l.Name] = struct{}{}
		})
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, nil
}

// ---------------- ChunkReader ----------------

type chunkReader struct {
	r *blockReader
}

func (cr *chunkReader) Close() error {
	cr.r.release()
	return nil
}

func (cr *chunkReader) Chunk(meta chunks.Meta) (chunkenc.Chunk, error) {
	sref, cid := chunks.HeadChunkRef(meta.Ref).Unpack()
	idx, ok := cr.r.lookupRef(storage.SeriesRef(sref))
	if !ok {
		return nil, storage.ErrNotFound
	}
	snap := cr.r.series[idx]
	if int(cid) >= len(snap.chunks) {
		return nil, fmt.Errorf("litehead: chunk id %d out of range (series has %d chunks)", cid, len(snap.chunks))
	}
	d := snap.chunks[cid]

	switch d.kind {
	case chunkSourceMmapped:
		chk, err := cr.r.head.chunkDiskMapper.Chunk(d.mmappedRef)
		if err != nil {
			return nil, errors.Wrap(err, "read mmapped chunk")
		}
		return chk, nil
	case chunkSourceOpen:
		return chunkenc.FromData(d.openEncoding, d.openBytes)
	default:
		return nil, fmt.Errorf("litehead: unknown chunk source %d", d.kind)
	}
}
