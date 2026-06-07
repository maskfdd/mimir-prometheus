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

	// ForcedFlushSealedChunks 是单条 series 允许持有的 sealed mmapped chunk 的硬上限。
	// 触达该值时 Append 路径会同步触发一次 forced flush 作为兜底保护。默认值见
	// defaultHardMmappedChunksPerSeries——当前取值保证该兜底是罕见事件，正常不应被触发。
	// 设为 <=0 时自动回落为默认值。
	ForcedFlushSealedChunks int

	// SoftFlushSealedChunks 是单条 series 的 sealed mmapped chunk 软告警阈值。
	// 超过该值时不会触发 forced flush，只会递增观测计数器（soft flush hits），
	// 用于提醒运维方外部 flush 节奏可能跟不上。要求 0 < Soft < ForcedFlush。
	// 设为 <=0 时自动回落为默认值。
	SoftFlushSealedChunks int

	EnableMemorySnapshotOnShutdown bool

	WALReplayConcurrency int

	WALSegmentSize int
	WALCompression wlog.CompressionType

	FlushCheckInterval time.Duration

	// EarlyFlushMinSeries 全局 series 数量达到此阈值时，即使时间跨度未达
	// ChunkRange*3/2，也提前触发一次 tryFlushAligned。这是借鉴 Mimir Block
	// Builder 的 CompactToReduceInMemorySeries 策略：当内存中 series 数量过多
	// 时，主动触发 compact 释放内存，避免"攒 3h 数据"导致的高内存占用。
	// <=0 表示关闭此功能。
	EarlyFlushMinSeries int64

	NoLockfile bool

	// SeriesLifecycleCallback specifies callbacks invoked during a series lifecycle.
	// PreCreation is called before creating a series (return non-nil error to reject).
	// PostCreation is called after a series is created.
	// PostDeletion is called after series are deleted.
	// If nil, a no-op callback is used.
	SeriesLifecycleCallback tsdb.SeriesLifecycleCallback
}

// DefaultOptions 返回默认选项，默认值对齐 tsdb.DefaultHeadOptions()。
func DefaultOptions() *Options {
	return &Options{
		ChunkRange:                     tsdb.DefaultBlockDuration,
		BlockDuration:                  tsdb.DefaultBlockDuration,
		WALSegmentSize:                 wlog.DefaultSegmentSize,
		WALCompression:                 wlog.CompressionNone,
		SamplesPerChunk:                tsdb.DefaultSamplesPerChunk,
		ForcedFlushSealedChunks:        defaultHardMmappedChunksPerSeries,
		SoftFlushSealedChunks:          defaultSoftMmappedChunksPerSeries,
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
	// forced flush 阈值：hard 必须 >= 2（理论最小可工作值），soft 必须严格小于 hard。
	// 若用户配置非法（soft >= hard 或其一 <=0），回落为默认。
	if o.ForcedFlushSealedChunks <= 0 {
		o.ForcedFlushSealedChunks = defaultHardMmappedChunksPerSeries
	}
	if o.ForcedFlushSealedChunks < 2 {
		o.ForcedFlushSealedChunks = 2
	}
	if o.SoftFlushSealedChunks <= 0 {
		o.SoftFlushSealedChunks = defaultSoftMmappedChunksPerSeries
	}
	if o.SoftFlushSealedChunks >= o.ForcedFlushSealedChunks {
		// soft 必须留出告警余量，否则 soft 事件会与 hard 事件同时触发，
		// 失去"提前预警"的语义。
		o.SoftFlushSealedChunks = o.ForcedFlushSealedChunks - 1
		if o.SoftFlushSealedChunks < 1 {
			o.SoftFlushSealedChunks = 1
		}
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
	if o.SeriesLifecycleCallback == nil {
		o.SeriesLifecycleCallback = noopSeriesLifecycleCallback{}
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
	initialized           atomic.Bool

	metrics *headMetrics
	opts    *Options
	wal     *wlog.WL
	logger  log.Logger

	appenderPool sync.Pool
	seriesPool   sync.Pool
	bufPool      sync.Pool
	// snapshotBufPool 复用 blockReader 冻结 open chunk 时需要的 *[]byte。
	// 生命周期：newBlockReader 拿取 -> 写入 chunkDescriptor.openBytes -> IndexReader
	// 与 ChunkReader 都 Close 后由 blockReader 归还。
	snapshotBufPool sync.Pool

	// series 索引。
	refTab   *refTable
	hashIdx  *hashIndex
	labelCat *labelCatalog

	chunkDiskMapper *chunks.ChunkDiskMapper

	seriesCallback tsdb.SeriesLifecycleCallback

	// appenderMtx prevents background self-compaction from snapshotting
	// uncommitted samples held by active appenders.
	appenderMtx sync.RWMutex
	flushMtx    sync.Mutex

	// compactor 复用 LeveledCompactor 实例，避免每次 flush 都重新分配。
	// 受 flushMtx 保护，无需额外同步。
	compactor *tsdb.LeveledCompactor

	// nextBatchGen 是全局原子计数器，为每个 appender 实例分配唯一的 batchGen。
	// 确保不同 appender 实例的 batchGen 不会冲突。
	nextBatchGen atomic.Uint64

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
		seriesCallback:  opts.SeriesLifecycleCallback,

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

	// 把 forced flush 的 soft/hard 配置暴露到 gauge，让 alerting 无需再硬编码阈值。
	h.metrics.mmappedChunksHardLimit.Set(float64(opts.ForcedFlushSealedChunks))
	h.metrics.mmappedChunksSoftLimit.Set(float64(opts.SoftFlushSealedChunks))

	h.seriesPool.New = func() any { return &memSeries{} }
	h.bufPool.New = func() any { b := make([]byte, 0, 1024); return &b }
	// snapshotBufPool 为 blockReader 的 open chunk 冻结字节复用 buffer。
	// 每次 flush 期间可能同时冻结上万条 series 的 open chunk，直接 make 会产生
	// 大量短命大对象；改为 pool 复用能显著降低 flush 期 heap 峰值与 GC 抖动。
	h.snapshotBufPool.New = func() any { b := make([]byte, 0, 512); return &b }
	h.appenderPool.New = func() any {
		return &appender{
			head:           h,
			pendingSeries:  make([]record.RefSeries, 0, 256),
			pendingSamples: make([]record.RefSample, 0, 1024),
			sampleSeries:   make([]*memSeries, 0, 1024),
			batchGen:       h.nextBatchGen.Inc(),
		}
	}

	return h, nil
}

// Init 回放 ChunkDiskMapper 和 WAL，恢复 series 状态。
// minValidTime 设置允许写入的最小时间戳下界。
func (h *Head) Init(minValidTime int64) error {
	h.setMinValidTime(minValidTime)
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
	h.initialized.Store(true)
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

// ErrNotInitialized 表示 Head 还未完成 Init 即被请求 Appender。
// 调用方应等待 Init 结束后再调 Appender，否则 WAL/CDM replay 未完成，
// refTab/hashIdx/labelCat 处于不一致状态，新写入会与即将回放的 ref 冲突。
var ErrNotInitialized = errors.New("litehead: Appender called before Init completed")

// Appender 返回一个写入 appender。
//
// 注意：appenderMtx 的 RLock **不**在此处获取，而是在 Commit 真正落盘
// 阶段细粒度获取（Rollback 若仅写 pendingSeries 也会获取）。这样避免
// 调用方持有 appender 后未 Commit/Rollback（例如 panic 路径）导致
// RLock 永远不释放，进而卡死 SelfCompact 的致命死锁。
func (h *Head) Appender(_ context.Context) storage.Appender {
	if !h.initialized.Load() {
		// 返回一个永远失败的 appender，而不是 panic——调用方可以安全 retry。
		return errAppender{err: ErrNotInitialized}
	}
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

// ForceFlush 强制 flush 所有内存数据到 block，不检查 compactable() 阈值。
// 与 SelfCompact 不同，ForceFlush 总是执行 flush，用于 idle/force compaction
// 场景下确保 liteDB 的数据被持久化，以便 TSDB 可以被关闭。
//
// 调用方须确保不会有并发写入（例如 ingester 已停止推送）。
func (h *Head) ForceFlush() error {
	h.appenderMtx.Lock()
	defer h.appenderMtx.Unlock()

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
// 调用前会先调 PreCreation callback，失败则拒绝创建。
func (h *Head) createSeries(hash uint64, lset labels.Labels) (*memSeries, error) {
	if err := h.seriesCallback.PreCreation(lset); err != nil {
		return nil, err
	}

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

	h.seriesCallback.PostCreation(lset)
	return s, nil
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

// noopSeriesLifecycleCallback is a no-op implementation of tsdb.SeriesLifecycleCallback.
type noopSeriesLifecycleCallback struct{}

func (noopSeriesLifecycleCallback) PreCreation(labels.Labels) error                     { return nil }
func (noopSeriesLifecycleCallback) PostCreation(labels.Labels)                          {}
func (noopSeriesLifecycleCallback) PostDeletion(map[chunks.HeadSeriesRef]labels.Labels) {}
