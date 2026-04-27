package litehead

import (
	"context"
	"fmt"
	"math"

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

// blockReader 让 litehead.Head 直接以 tsdb.BlockReader 身份
// 喂给 LeveledCompactor.Write，省掉 "临时 Head + WAL 回放" 那一趟复制。
//
// 生命周期：每次 compactHeadWindow 调用时新建，写完 block 就丢。
// 快照在 newBlockReader 里一次性采集；后续 Postings / Series /
// Chunk 都从这份快照取，不再回 Head 的主索引（refTab/hashIdx），避免
// flush 期间的并发追加 / spill 把状态改掉。
//
// 并发模型：flush 与 appender 之间靠 h.flushMtx 互斥 compact 本身，
// 但 append 仍可能并行写同一条 series 的 open chunk。Chunk(meta) 读
// open chunk 字节时用 s.mu 保护 + bytes copy，避开和 appender 的竞争。
// mmappedChunks 一旦 spill 到 ChunkDiskMapper 就不可变，直接 cdm.Chunk
// 读即可。
type blockReader struct {
	head       *Head
	mint, maxt int64

	meta tsdb.BlockMeta

	// series 快照：按 labels 排序（AllPostings 和 SortedPostings 可以直接用）。
	// Series(ref) 用 refToIdx 映射到这份快照。
	series    []*seriesSnapshot
	refToIdx  map[storage.SeriesRef]int
	symbolSet []string // 去重且已排序的 symbols
}

// seriesSnapshot 是一条 series 在 flush 开始那一瞬间的只读投影。
// 构造完成后 chunks 数组本身不变；mmapped chunk 直接读 ChunkDiskMapper，
// open chunk 的字节在快照阶段就冻结下来，避免后续 spill / append 改写。
type seriesSnapshot struct {
	ref  chunks.HeadSeriesRef
	lset labels.Labels
	// chunks 按 chunk id 顺序排列：先是 mmappedChunks[0..n)，最后 1 个
	// 是 open chunk（若存在）。chunk ref 用 chunks.NewHeadChunkRef(ref, id)。
	chunks []chunkDescriptor
}

// chunkDescriptor 是 chunks.Meta 的最小信息。Chunk() 方法按 kind 分发。
type chunkDescriptor struct {
	minTime int64
	maxTime int64 // open chunk 的 maxTime 在 Meta 里会被改成 MaxInt64

	kind         chunkSource
	mmappedRef   chunks.ChunkDiskMapperRef // 仅当 kind == chunkSourceMmapped 时有效
	openEncoding chunkenc.Encoding         // 仅当 kind == chunkSourceOpen 时有效
	openBytes    []byte                    // 仅当 kind == chunkSourceOpen 时有效
}

type chunkSource uint8

const (
	chunkSourceMmapped chunkSource = iota
	chunkSourceOpen
)

// newBlockReader 构造快照。
// 不能在 refTab.forEach 的回调里再取主锁，参见 refTab.forEach 注释。
func newBlockReader(h *Head, mint, maxt int64) *blockReader {
	r := &blockReader{
		head:     h,
		mint:     mint,
		maxt:     maxt,
		refToIdx: make(map[storage.SeriesRef]int, 1024),
	}

	symbols := make(map[string]struct{}, 256)

	h.refTab.forEach(func(s *memSeries) {
		s.mu.Lock()
		if !seriesOverlapsWindowLocked(s, mint, maxt) {
			s.mu.Unlock()
			return
		}
		descs := make([]chunkDescriptor, 0, int(s.mmappedChunksCount)+1)
		for i := 0; i < int(s.mmappedChunksCount); i++ {
			mc := s.mmappedChunks[i]
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
				frozen := make([]byte, len(b))
				copy(frozen, b)
				// 对齐标准 Head 的对外语义：仍把 open chunk 暴露成“可增长”块。
				// 真正的字节和编码已经在快照阶段冻结，避免 Chunk() 再回看 live 状态。
				descs = append(descs, chunkDescriptor{
					minTime:      openMinT,
					maxTime:      math.MaxInt64,
					kind:         chunkSourceOpen,
					openEncoding: s.openChunk.Encoding(),
					openBytes:    frozen,
				})
			}
		}
		if len(descs) == 0 {
			s.mu.Unlock()
			return
		}
		lset := h.labelCat.get(s.labelsID)
		snap := &seriesSnapshot{
			ref:    s.ref,
			lset:   lset,
			chunks: descs,
		}
		s.mu.Unlock()

		lset.Range(func(l labels.Label) {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		})

		r.refToIdx[storage.SeriesRef(snap.ref)] = len(r.series)
		r.series = append(r.series, snap)
	})

	// 按 labels.Compare 排序；compactor 要求 SortedPostings 的顺序稳定。
	slices.SortFunc(r.series, func(a, b *seriesSnapshot) int {
		return labels.Compare(a.lset, b.lset)
	})
	for i := range r.series {
		r.refToIdx[storage.SeriesRef(r.series[i].ref)] = i
	}

	r.symbolSet = make([]string, 0, len(symbols))
	for s := range symbols {
		r.symbolSet = append(r.symbolSet, s)
	}
	slices.Sort(r.symbolSet)

	// ULID 只在日志/错误里露脸；真正写入 meta.json 的 ULID 由 compactor
	// 的 outBlocks[0].meta 掌控，与这个无关。
	r.meta = tsdb.BlockMeta{
		ULID:    ulid.ULID{},
		MinTime: mint,
		MaxTime: maxt,
	}
	return r
}

// seriesOverlapsWindowLocked 判断 series 在 [mint, maxt] 范围内有没有可读样本。
// 调用方必须持有 s.mu。
func seriesOverlapsWindowLocked(s *memSeries, mint, maxt int64) bool {
	for i := 0; i < int(s.mmappedChunksCount); i++ {
		mc := s.mmappedChunks[i]
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
	return &indexReader{r: r}, nil
}

func (r *blockReader) Chunks() (tsdb.ChunkReader, error) {
	return &chunkReader{r: r}, nil
}

func (r *blockReader) Tombstones() (tombstones.Reader, error) {
	return tombstones.NewMemTombstones(), nil
}

// ---------------- IndexReader ----------------

type indexReader struct {
	r *blockReader
}

func (ir *indexReader) Close() error { return nil }

func (ir *indexReader) Symbols() index.StringIter {
	return index.NewStringListIter(ir.r.symbolSet)
}

// Postings 只支持 AllPostingsKey("" / "")，这是 compactor 唯一会调的形态。
// 其它 name/value 精确查询在本 reader 场景下不会被触发；即便意外被调到，
// 返回 EmptyPostings 也只是让 compactor 少写一点，不会 panic。
func (ir *indexReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	switch len(values) {
	case 0:
		return index.EmptyPostings(), nil
	case 1:
		refs := make([]storage.SeriesRef, 0, len(ir.r.series))
		for _, s := range ir.r.series {
			if s.lset.Get(name) == values[0] {
				refs = append(refs, storage.SeriesRef(s.ref))
			}
		}
		return index.NewListPostings(refs), nil
	default:
		res := make([]index.Postings, 0, len(values))
		for _, value := range values {
			p, err := ir.Postings(ctx, name, value)
			if err != nil {
				return nil, err
			}
			if !index.IsEmptyPostingsType(p) {
				res = append(res, p)
			}
		}
		return index.Merge(ctx, res...), nil
	}
}

func (ir *indexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*seriesSnapshot, 0, 128)
	for p.Next() {
		idx, ok := ir.r.refToIdx[p.At()]
		if !ok {
			continue
		}
		series = append(series, ir.r.series[idx])
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(err)
	}
	slices.SortFunc(series, func(a, b *seriesSnapshot) int {
		return labels.Compare(a.lset, b.lset)
	})
	refs := make([]storage.SeriesRef, 0, len(series))
	for _, s := range series {
		refs = append(refs, storage.SeriesRef(s.ref))
	}
	return index.NewListPostings(refs)
}

func (ir *indexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	var out []storage.SeriesRef
	for p.Next() {
		ref := p.At()
		idx, ok := ir.r.refToIdx[ref]
		if !ok {
			continue
		}
		if labels.StableHash(ir.r.series[idx].lset)%shardCount != shardIndex {
			continue
		}
		out = append(out, ref)
	}
	return index.NewListPostings(out)
}

func (ir *indexReader) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	idx, ok := ir.r.refToIdx[ref]
	if !ok {
		return storage.ErrNotFound
	}
	snap := ir.r.series[idx]

	builder.Reset()
	snap.lset.Range(func(l labels.Label) {
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

// 下面这些方法 compactor 不会调，但 IndexReader 接口要求齐全。
// 给出能 work 的朴素实现以防外部代码误调。

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
		if v := s.lset.Get(name); v != "" {
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
		s.lset.Range(func(l labels.Label) {
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
		match := true
		for _, m := range ms {
			if !m.Matches(s.lset.Get(m.Name)) {
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
	idx, ok := ir.r.refToIdx[id]
	if !ok {
		return "", storage.ErrNotFound
	}
	v := ir.r.series[idx].lset.Get(label)
	if v == "" {
		return "", storage.ErrNotFound
	}
	return v, nil
}

func (ir *indexReader) LabelNamesFor(ctx context.Context, ids ...storage.SeriesRef) ([]string, error) {
	set := make(map[string]struct{}, 16)
	for _, id := range ids {
		idx, ok := ir.r.refToIdx[id]
		if !ok {
			return nil, storage.ErrNotFound
		}
		ir.r.series[idx].lset.Range(func(l labels.Label) {
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

func (cr *chunkReader) Close() error { return nil }

func (cr *chunkReader) Chunk(meta chunks.Meta) (chunkenc.Chunk, error) {
	sref, cid := chunks.HeadChunkRef(meta.Ref).Unpack()
	idx, ok := cr.r.refToIdx[storage.SeriesRef(sref)]
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
