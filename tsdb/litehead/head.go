// Package litehead 实现了一个写入专用的轻量 TSDB Head，可替代标准
// tsdb.Head 用于 mimir-ingester 等只写场景。
//
// 与标准 Head 相比，省略了 postings、查询迭代器、isolation、OOO 等查询侧
// 结构。每条 series 只保留写入所需的最小状态（ref、labels 在 arena 中的
// 位置、lastTs、open chunk）；sealed chunk 立即 mmap 到磁盘。
//
// 生命周期：
//
//	NewHead  -> 打开 WAL + ChunkDiskMapper
//	Init     -> 回放 WAL / snapshot 恢复状态
//	Appender -> 追加样本、写 WAL、必要时切 chunk 并 spill
//	Flush    -> compact 成 block + truncate WAL（外部调度）
//	Close    -> Flush + Checkpoint + 释放资源
//
// Flush 路径直接把 litehead 的 blockReader 喂给 LeveledCompactor.Write，
// 无需临时 Head + WAL 回放。
//
// 详细设计见 tsdb/litehead/docs/write_only_head_design.md。
package litehead

import (
	"context"
	"math"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

var _ storage.Storage = (*Head)(nil)

const (
	walSubDir        = "wal"
	chunksHeadSubDir = "chunks_head"
	lockfileName     = "tsdb"
)

// Options 控制 Head 的行为。字段与默认值对齐标准 HeadOptions。
type Options struct {
	ChunkRange    int64
	BlockDuration int64

	ChunkWriteBufferSize int
	ChunkWriteQueueSize  int

	SamplesPerChunk int

	EnableMemorySnapshotOnShutdown bool

	WALReplayConcurrency int

	WALSegmentSize int
	WALCompression wlog.CompressionType

	FlushCheckInterval time.Duration

	NoLockfile bool
}

// DefaultOptions 返回默认选项，默认值对齐 tsdb.DefaultHeadOptions()。
func DefaultOptions() *Options {
	return &Options{
		ChunkRange:                     tsdb.DefaultBlockDuration,
		BlockDuration:                  tsdb.DefaultBlockDuration,
		WALSegmentSize:                 wlog.DefaultSegmentSize,
		WALCompression:                 wlog.CompressionNone,
		SamplesPerChunk:                tsdb.DefaultSamplesPerChunk,
		FlushCheckInterval:             time.Minute,
		ChunkWriteBufferSize:           chunks.DefaultWriteBufferSize,
		ChunkWriteQueueSize:            chunks.DefaultWriteQueueSize,
		WALReplayConcurrency:           runtime.GOMAXPROCS(0),
		EnableMemorySnapshotOnShutdown: false,
		NoLockfile:                     false,
	}
}

func (o *Options) validate() *Options {
	if o == nil {
		o = DefaultOptions()
	}
	if o.ChunkRange <= 0 {
		o.ChunkRange = DefaultOptions().ChunkRange
	}
	if o.BlockDuration <= 0 {
		o.BlockDuration = o.ChunkRange
	}
	if o.WALSegmentSize <= 0 {
		o.WALSegmentSize = wlog.DefaultSegmentSize
	}
	if o.WALCompression == "" {
		o.WALCompression = wlog.CompressionNone
	}
	if o.SamplesPerChunk <= 0 {
		o.SamplesPerChunk = tsdb.DefaultSamplesPerChunk
	}
	if o.FlushCheckInterval <= 0 {
		o.FlushCheckInterval = time.Minute
	}
	if o.ChunkWriteBufferSize <= 0 {
		o.ChunkWriteBufferSize = chunks.DefaultWriteBufferSize
	}
	if o.WALReplayConcurrency <= 0 {
		o.WALReplayConcurrency = runtime.GOMAXPROCS(0)
	}
	return o
}

// Head 是 litehead 的入口，实现 storage.Storage 的写入子集。
// 字段排布对齐标准 tsdb.Head。
type Head struct {
	// --- 原子字段（对齐标准 Head 布局）---
	chunkRange            atomic.Int64
	numSeries             atomic.Uint64
	minTime, maxTime      atomic.Int64
	minValidTime          atomic.Int64
	lastWALTruncationTime atomic.Int64
	lastSeriesID          atomic.Uint64

	metrics *headMetrics
	opts    *Options
	wal     *wlog.WL
	logger  log.Logger

	appenderPool sync.Pool
	seriesPool   sync.Pool
	bufPool      sync.Pool

	// series 索引。
	refTab   *refTable
	hashIdx  *hashIndex
	labelCat *labelCatalog

	chunkDiskMapper *chunks.ChunkDiskMapper

	flushMtx sync.Mutex

	dir    string
	locker *tsdbutil.DirLocker
}

// NewHead 创建 Head 实例。创建后需调用 Init 回放 WAL 恢复状态。
func NewHead(logger log.Logger, reg prometheus.Registerer, dir string, opts *Options) (*Head, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	opts = opts.validate()

	locker, err := tsdbutil.NewDirLocker(dir, lockfileName, logger, reg)
	if err != nil {
		return nil, err
	}
	if !opts.NoLockfile {
		if err := locker.Lock(); err != nil {
			return nil, err
		}
	}

	w, err := wlog.NewSize(logger, reg, filepath.Join(dir, walSubDir), opts.WALSegmentSize, opts.WALCompression)
	if err != nil {
		return nil, errors.Wrap(err, "open WAL")
	}

	cdm, err := chunks.NewChunkDiskMapper(reg, filepath.Join(dir, chunksHeadSubDir), chunkenc.NewPool(), opts.ChunkWriteBufferSize, opts.ChunkWriteQueueSize)
	if err != nil {
		return nil, errors.Wrap(err, "open chunk disk mapper")
	}

	h := &Head{
		logger:          logger,
		dir:             dir,
		opts:            opts,
		locker:          locker,
		wal:             w,
		chunkDiskMapper: cdm,

		refTab:   newRefTable(),
		hashIdx:  newHashIndex(),
		labelCat: newLabelCatalog(),

		metrics: newHeadMetrics(reg),
	}
	h.chunkRange.Store(opts.ChunkRange)
	h.minTime.Store(math.MaxInt64)
	h.maxTime.Store(math.MinInt64)
	h.minValidTime.Store(math.MinInt64)
	h.lastWALTruncationTime.Store(math.MinInt64)

	h.seriesPool.New = func() any { return &memSeries{} }
	h.bufPool.New = func() any { b := make([]byte, 0, 1024); return &b }
	h.appenderPool.New = func() any {
		return &appender{
			head:           h,
			pendingSeries:  make([]record.RefSeries, 0, 256),
			pendingSamples: make([]record.RefSample, 0, 1024),
			sampleSeries:   make([]*memSeries, 0, 1024),
		}
	}

	return h, nil
}

// Init 回放 ChunkDiskMapper 和 WAL，恢复 series 状态。
func (h *Head) Init() error {
	// 先回放 ChunkDiskMapper，再回放 WAL。
	if err := h.replayChunkDiskMapper(); err != nil {
		level.Warn(h.logger).Log("msg", "chunk disk mapper replay returned error", "err", err)
	}
	if err := h.replayWAL(); err != nil {
		level.Warn(h.logger).Log("msg", "WAL replay error, attempting repair", "err", err)
		if repairErr := h.wal.Repair(err); repairErr != nil {
			return tsdb_errors.NewMulti(errors.Wrap(err, "replay WAL"), errors.Wrap(repairErr, "repair WAL")).Err()
		}
	}
	return nil
}

// Close 按配置写 snapshot，执行最后一次 Flush/Checkpoint，释放资源。
func (h *Head) Close() error {
	errs := tsdb_errors.NewMulti()

	// 在 flush 前写 snapshot，此时 refTab 中还有所有 series。
	if h.opts.EnableMemorySnapshotOnShutdown {
		if err := h.writeSnapshot(); err != nil {
			level.Warn(h.logger).Log("msg", "write snapshot failed", "err", err)
		}
	}

	// 最后一次 flush。
	if err := h.tryFlushAll(); err != nil {
		level.Warn(h.logger).Log("msg", "final flush failed", "err", err)
	}

	// WAL checkpoint，减少下次启动的 replay 时间。
	if err := h.truncateWAL(math.MaxInt64); err != nil {
		level.Warn(h.logger).Log("msg", "shutdown checkpoint failed", "err", err)
	}

	if err := h.chunkDiskMapper.Close(); err != nil {
		errs.Add(errors.Wrap(err, "close chunk disk mapper"))
	}
	if err := h.wal.Close(); err != nil {
		errs.Add(errors.Wrap(err, "close WAL"))
	}
	if err := h.locker.Release(); err != nil {
		errs.Add(errors.Wrap(err, "release lock"))
	}

	h.metrics.unregister()

	return errs.Err()
}

// Dir 返回数据目录。
func (h *Head) Dir() string { return h.dir }

// NumSeries 返回当前活跃 series 数量（O(1)）。
func (h *Head) NumSeries() uint64 { return h.numSeries.Load() }

func (h *Head) MinTime() int64 {
	v := h.minTime.Load()
	if v == math.MaxInt64 {
		return math.MinInt64
	}
	return v
}

func (h *Head) MaxTime() int64 {
	v := h.maxTime.Load()
	if v == math.MinInt64 {
		return math.MinInt64
	}
	return v
}

// appendableMinValidTime 返回当前允许写入的最小时间。
func (h *Head) appendableMinValidTime() int64 {
	minValid := h.minValidTime.Load()
	maxt := h.MaxTime()
	if maxt == math.MinInt64 {
		return minValid
	}

	cwEnd := maxt - h.chunkRange.Load()/2
	if cwEnd > minValid {
		return cwEnd
	}
	return minValid
}

func (h *Head) setMinValidTime(t int64) {
	for {
		cur := h.minValidTime.Load()
		if t <= cur {
			return
		}
		if h.minValidTime.CompareAndSwap(cur, t) {
			return
		}
	}
}

// StartTime 实现 storage.Storage 接口。
func (h *Head) StartTime() (int64, error) {
	if mt := h.MinTime(); mt != math.MinInt64 {
		return mt, nil
	}
	return int64(0), nil
}

// Appender 返回一个写入 appender。
func (h *Head) Appender(_ context.Context) storage.Appender {
	return h.appenderPool.Get().(storage.Appender)
}

// ErrQuerierUnsupported 表示 litehead 不支持查询。
var ErrQuerierUnsupported = errors.New("litehead: querier is not supported; query flushed blocks instead")

func (h *Head) Querier(int64, int64) (storage.Querier, error) {
	return nil, ErrQuerierUnsupported
}

func (h *Head) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return nil, ErrQuerierUnsupported
}

func (h *Head) ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error) {
	return nil, ErrQuerierUnsupported
}

// ChunkRange 返回配置的 chunk 时间跨度（ms）。
func (h *Head) ChunkRange() int64 { return h.chunkRange.Load() }

// IsCompactable 返回内存数据是否满足 compact 条件。
func (h *Head) IsCompactable() bool { return h.compactable() }

// Size 返回磁盘占用的近似字节数（WAL + ChunkDiskMapper）。
func (h *Head) Size() int64 {
	var walSize int64
	if h.wal != nil {
		walSize, _ = h.wal.Size()
	}
	cdmSize, _ := h.chunkDiskMapper.Size()
	return walSize + cdmSize
}

func (h *Head) String() string { return "litehead" }

// Flush 把内存中可 flush 的数据写为 block 并截断 WAL。
func (h *Head) Flush() error {
	return h.tryFlushAll()
}

// AppendableMinValidTime 返回当前允许写入的最小时间。
func (h *Head) AppendableMinValidTime() int64 {
	return h.appendableMinValidTime()
}

// Meta 返回 Head 的 BlockMeta。
func (h *Head) Meta() tsdb.BlockMeta {
	var id [16]byte
	copy(id[:], "______lite______")
	return tsdb.BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    ulid.ULID(id),
		Stats: tsdb.BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// Stats 返回 Head 的统计信息。
func (h *Head) Stats(_ string, _ int) *tsdb.Stats {
	return &tsdb.Stats{
		NumSeries: h.NumSeries(),
		MaxTime:   h.MaxTime(),
		MinTime:   h.MinTime(),
	}
}

// PostingsCardinalityStats 返回 nil，litehead 不维护 postings 索引。
func (h *Head) PostingsCardinalityStats(_ string, _ int) *index.PostingsStats {
	return nil
}

// createSeries 为新 labels 分配 ref 并注册到 refTable / hashIndex。
func (h *Head) createSeries(hash uint64, lset labels.Labels) *memSeries {
	labelsID := h.labelCat.put(lset)
	ref := chunks.HeadSeriesRef(h.lastSeriesID.Inc())

	s := h.seriesPool.Get().(*memSeries)
	*s = memSeries{
		ref:      ref,
		labelsID: labelsID,
		lastTs:   math.MinInt64,
	}
	h.refTab.set(ref, s)
	h.hashIdx.put(hash, ref, labelsID)
	h.numSeries.Inc()
	h.metrics.seriesActive.Inc()
	return s
}

// rangeForTimestamp 返回按 width 对齐的时间窗上界。
func rangeForTimestamp(t, width int64) int64 {
	return (t/width)*width + width
}

// updateMinMaxTime 原子更新 Head 的时间范围。
func (h *Head) updateMinMaxTime(t int64) {
	for {
		cur := h.minTime.Load()
		if t >= cur {
			break
		}
		if h.minTime.CompareAndSwap(cur, t) {
			break
		}
	}
	for {
		cur := h.maxTime.Load()
		if t <= cur {
			break
		}
		if h.maxTime.CompareAndSwap(cur, t) {
			break
		}
	}
}
