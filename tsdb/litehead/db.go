// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package litehead

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// 目录结构：
//
//   <dir>/
//     wal/              WAL 段；Series / Samples / Histogram / Checkpoint 都在这里
//     chunks_head/      ChunkDiskMapper 写的 head chunk 文件（sealed chunk 字节）
//     <ULID>/           每次 Flush 成功后生成的 block 目录
//
// 这里的目录名与原生 tsdb/head 保持一致，方便外部工具复用。

const (
	walSubDir        = "wal"
	chunksHeadSubDir = "chunks_head"
	lockfileName     = "litehead"
)

// Options 控制 LiteHead 的行为。大部分字段与 tsdb.HeadOptions 语义
// 对齐，避免外部使用者二次学习。
type Options struct {
	// ChunkRange 决定 chunk 的时间跨度与 block 时间窗（ms）。
	ChunkRange int64

	// BlockDuration 是单次 Flush 切出的 block 时间跨度（ms）。默认等于
	// ChunkRange。为了与原生 Head 行为对齐，建议保持默认。
	BlockDuration int64

	// WALSegmentSize：单个 WAL 段的最大字节；<=0 表示使用 wlog 默认。
	WALSegmentSize int
	// WALCompression：WAL 段压缩算法。
	WALCompression wlog.CompressionType

	// SamplesPerChunk：每个 chunk 的目标样本数。达到 2x 时强制切 chunk。
	SamplesPerChunk int

	// FlushCheckInterval：后台 goroutine 检查是否需要 Flush 的周期。
	FlushCheckInterval time.Duration

	// ChunkWriteBufferSize / ChunkWriteQueueSize：传给 ChunkDiskMapper。
	ChunkWriteBufferSize int
	ChunkWriteQueueSize  int

	// EnableMemorySnapshotOnShutdown 控制是否在关机时额外生成内存快照，
	// 使下次启动可以跳过大部分 WAL 回放。
	//
	// 现状（TODO）：本选项仅作为兼容原生 tsdb.Options 的占位保留。当前
	// 实现里开启该开关后，Close 时只会触发一次 WAL checkpoint 作为兜底，
	// 并不会真正生成 chunk_snapshot 目录。真正的 shutdown snapshot（以
	// 及对应的 replay 快速路径）留待后续版本补齐。
	EnableMemorySnapshotOnShutdown bool

	// NoLockfile：禁用目录锁（方便单测）。
	NoLockfile bool
}

// DefaultOptions 返回 LiteHead 的默认选项。
func DefaultOptions() *Options {
	return &Options{
		ChunkRange:                     2 * 60 * 60 * 1000, // 2h
		BlockDuration:                  2 * 60 * 60 * 1000,
		WALSegmentSize:                 wlog.DefaultSegmentSize,
		WALCompression:                 wlog.CompressionNone,
		SamplesPerChunk:                120,
		FlushCheckInterval:             time.Minute,
		ChunkWriteBufferSize:           4 * 1024 * 1024,
		ChunkWriteQueueSize:            0,
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
		o.SamplesPerChunk = 120
	}
	if o.FlushCheckInterval <= 0 {
		o.FlushCheckInterval = time.Minute
	}
	if o.ChunkWriteBufferSize <= 0 {
		o.ChunkWriteBufferSize = 4 * 1024 * 1024
	}
	return o
}

// DB 是 LiteHead 的对外入口，实现了 storage.Storage 的子集，可以直接
// 作为 mimir-ingester 中 tsdb.Head 的替代。
//
// 字段命名尽可能向标准 tsdb.Head 对齐：
//   - minTime / maxTime（不是 minT/maxT）
//   - chunkDiskMapper（不是 chunkDisk）
//
// 这样读代码时可以直接和 tsdb/head.go 对照。
type DB struct {
	logger log.Logger
	dir    string
	opts   *Options
	locker *tsdbutil.DirLocker

	wal             *wlog.WL
	chunkDiskMapper *chunks.ChunkDiskMapper

	nextRef *atomic.Uint64

	refTab     *refTable
	hashIdx    *hashIndex
	labelCat   *labelCatalog
	seriesPool sync.Pool

	// 最小/最大样本时间，供 compactHead 判定是否满足时间窗。
	// 命名对齐 tsdb.Head.minTime / maxTime。
	minTime atomic.Int64
	maxTime atomic.Int64

	// minValidTime 对齐标准 Head：任何 t < minValidTime 的样本都不能再写入，
	// 用来阻止 flush/truncate 窗口内的数据被并发 append 重新写回。
	minValidTime atomic.Int64

	appenderPool sync.Pool
	bufPool      sync.Pool

	// 后台 flush goroutine。
	donec    chan struct{}
	stopc    chan struct{}
	flushMtx sync.Mutex

	metrics *dbMetrics
}

// Open 打开或创建一个 LiteHead 实例。dir 为顶层目录，会在其下创建
// wal/ 和 chunks_head/ 子目录。
func Open(logger log.Logger, reg prometheus.Registerer, dir string, opts *Options) (*DB, error) {
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

	db := &DB{
		logger:          logger,
		dir:             dir,
		opts:            opts,
		locker:          locker,
		wal:             w,
		chunkDiskMapper: cdm,

		nextRef:  atomic.NewUint64(0),
		refTab:   newRefTable(),
		hashIdx:  newHashIndex(),
		labelCat: newLabelCatalog(),

		donec: make(chan struct{}),
		stopc: make(chan struct{}),

		metrics: newDBMetrics(reg),
	}
	db.minTime.Store(math.MaxInt64)
	db.maxTime.Store(math.MinInt64)
	db.minValidTime.Store(math.MinInt64)

	db.seriesPool.New = func() interface{} { return &memSeries{} }
	db.bufPool.New = func() interface{} { b := make([]byte, 0, 1024); return &b }
	db.appenderPool.New = func() interface{} {
		return &appender{
			db:             db,
			pendingSeries:  make([]record.RefSeries, 0, 256),
			pendingSamples: make([]record.RefSample, 0, 1024),
			sampleSeries:   make([]*memSeries, 0, 1024),
		}
	}

	// 先回放 ChunkDiskMapper（触发 mmap 文件打开、读取最大 ref），再回放 WAL。
	// 这样 WAL replay 就能基于已知的 chunkRef 决定是否跳过“孤儿 sealed”。
	if err := db.replayChunkDiskMapper(); err != nil {
		level.Warn(logger).Log("msg", "chunk disk mapper replay returned error", "err", err)
	}
	if err := db.replayWAL(); err != nil {
		level.Warn(logger).Log("msg", "WAL replay error, attempting repair", "err", err)
		if repairErr := w.Repair(err); repairErr != nil {
			return nil, tsdb_errors.NewMulti(errors.Wrap(err, "replay WAL"), errors.Wrap(repairErr, "repair WAL")).Err()
		}
	}

	go db.run()
	return db, nil
}

// Close 触发一次 Flush/Checkpoint，并释放底层资源。
func (db *DB) Close() error {
	close(db.stopc)
	<-db.donec

	errs := tsdb_errors.NewMulti()

	// 尝试最后一次 flush，把内存里的数据固化成 block。失败不阻塞关机。
	if err := db.tryFlushAll(); err != nil {
		level.Warn(db.logger).Log("msg", "final flush failed", "err", err)
	}

	// 触发一次 WAL checkpoint，减少下次启动的 replay 时间。
	// TODO：当 EnableMemorySnapshotOnShutdown 为 true 时，这里应该写出
	// 完整的内存快照（类似 tsdb.Head.ChunkSnapshot），启动时读取后可以
	// 跳过大部分 WAL 重放。本期先用 checkpoint 兑底，保证数据不丢。
	if db.opts.EnableMemorySnapshotOnShutdown {
		if err := db.truncateWAL(math.MaxInt64); err != nil {
			level.Warn(db.logger).Log("msg", "shutdown checkpoint failed", "err", err)
		}
	}

	if err := db.chunkDiskMapper.Close(); err != nil {
		errs.Add(errors.Wrap(err, "close chunk disk mapper"))
	}
	if err := db.wal.Close(); err != nil {
		errs.Add(errors.Wrap(err, "close WAL"))
	}
	if err := db.locker.Release(); err != nil {
		errs.Add(errors.Wrap(err, "release lock"))
	}

	db.metrics.unregister()

	return errs.Err()
}

// Dir 返回数据目录。
func (db *DB) Dir() string { return db.dir }

// NumSeries 返回当前活跃 series 的近似数量。
func (db *DB) NumSeries() int { return db.refTab.len() }

// MinTime / MaxTime 返回当前内存中样本的时间范围。
func (db *DB) MinTime() int64 {
	v := db.minTime.Load()
	if v == math.MaxInt64 {
		return math.MinInt64
	}
	return v
}

func (db *DB) MaxTime() int64 {
	v := db.maxTime.Load()
	if v == math.MinInt64 {
		return math.MinInt64
	}
	return v
}

// appendableMinValidTime 返回当前允许写入的最小时间。
// 语义对齐标准 Head：样本既不能落到已 flush/truncate 的边界之前，也不能
// 落到当前可能正被 compact 的 compaction window 里。
func (db *DB) appendableMinValidTime() int64 {
	minValid := db.minValidTime.Load()
	maxt := db.MaxTime()
	if maxt == math.MinInt64 {
		return minValid
	}

	cwEnd := maxt - db.opts.ChunkRange/2
	if cwEnd > minValid {
		return cwEnd
	}
	return minValid
}

func (db *DB) setMinValidTime(t int64) {
	for {
		cur := db.minValidTime.Load()
		if t <= cur {
			return
		}
		if db.minValidTime.CompareAndSwap(cur, t) {
			return
		}
	}
}

// StartTime 用于兼容 storage.Storage 接口。
func (db *DB) StartTime() (int64, error) {
	if mt := db.MinTime(); mt != math.MinInt64 {
		return mt, nil
	}
	return int64(0), nil
}

// Appender 返回一个新的写入 appender。
func (db *DB) Appender(_ context.Context) storage.Appender {
	return db.appenderPool.Get().(storage.Appender)
}

// ErrQuerierUnsupported 表示 LiteHead 不支持查询。
var ErrQuerierUnsupported = errors.New("litehead: querier is not supported; query flushed blocks instead")

// Querier 实现 storage.Storage 接口。LiteHead 本身不提供查询能力。
func (db *DB) Querier(int64, int64) (storage.Querier, error) {
	return nil, ErrQuerierUnsupported
}

// ChunkQuerier 实现 storage.Storage 接口。
func (db *DB) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return nil, ErrQuerierUnsupported
}

// ExemplarQuerier 实现 storage.Storage 接口。
func (db *DB) ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error) {
	return nil, ErrQuerierUnsupported
}

// createSeries 为新 labels 分配 ref + labelsID，并注册到 refTable / hashIndex。
// 调用方需要保证同一 labels 不会并发创建（通过 hashIndex 的读锁 + 冷路径
// 的乐观+CAS 式注册）。
func (db *DB) createSeries(hash uint64, lset labels.Labels) *memSeries {
	// 1. labels 写入 arena。
	labelsID := db.labelCat.put(lset)

	// 2. 分配 ref。注意：nextRef 单调递增，首次分配从 1 开始。
	ref := chunks.HeadSeriesRef(db.nextRef.Inc())

	s := db.seriesPool.Get().(*memSeries)
	*s = memSeries{
		ref:      ref,
		labelsID: labelsID,
		lastTs:   math.MinInt64,
	}
	db.refTab.set(ref, s)
	db.hashIdx.put(hash, ref, labelsID)
	db.metrics.seriesActive.Inc()
	return s
}

// rangeForTimestamp 与 tsdb 内部同名函数对齐，返回按 chunkRange 对齐的“下一
// 个上界”。
func rangeForTimestamp(t, width int64) int64 {
	return (t/width)*width + width
}

// updateMinMaxTime 以原子方式更新 Head 的时间范围。
// 命名对齐 tsdb.Head.updateMinMaxTime。
func (db *DB) updateMinMaxTime(t int64) {
	for {
		cur := db.minTime.Load()
		if t >= cur {
			break
		}
		if db.minTime.CompareAndSwap(cur, t) {
			break
		}
	}
	for {
		cur := db.maxTime.Load()
		if t <= cur {
			break
		}
		if db.maxTime.CompareAndSwap(cur, t) {
			break
		}
	}
}
