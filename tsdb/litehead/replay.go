// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package litehead

import (
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// replayChunkDiskMapper 负责：
// 1. 初始化 mmap 文件，构建 chunk 文件目录结构；
// 2. 遍历所有 head chunk，拿到最大的 HeadSeriesRef，用于恢复 nextRef；
//
// 这里不重建 sealed[] 数组——那部分会在 WAL replay 阶段结合 series
// 元数据重建。本期实现里 sealed[] 的恢复被有意省略（稳态下每条 series
// 只有 1~2 个 sealed chunk，WAL 已经记录了样本）。
// TODO: 把 ChunkDiskMapper 中属于当前 Head 未 flush 的 chunk 关联回 writeSeries。
func (db *DB) replayChunkDiskMapper() error {
	var maxRef chunks.HeadSeriesRef
	iterFn := func(seriesRef chunks.HeadSeriesRef, _ chunks.ChunkDiskMapperRef, _, _ int64, _ uint16, _ chunkenc.Encoding, _ bool) error {
		if seriesRef > maxRef {
			maxRef = seriesRef
		}
		return nil
	}
	err := db.chunkDiskMapper.IterateAllChunks(iterFn)
	if err != nil {
		return errors.Wrap(err, "iterate chunk disk mapper")
	}
	// 这里只是做个兜底：后续 WAL replay 还会继续更新 nextRef。
	if uint64(maxRef) > db.nextRef.Load() {
		db.nextRef.Store(uint64(maxRef))
	}
	return nil
}

// replayWAL 从 checkpoint + 所有 WAL 段恢复 lite head 的最小状态：
// 1. refTable / hashIndex / labelCatalog 里的 ref -> series 映射（来自 Series 记录）；
// 2. 每条 series 的 lastTs，以及全库 minT / maxT（来自 Samples 记录）；
// 3. nextRef（单调递增的整数）。
//
// 与原生 tsdb.Head.loadWAL 相比，这里 *有意* 不做的事情：
// - 不重建倒排索引：litehead 不提供查询；
// - 不重建 open chunk：下条样本到来时 appender 会懒分配新 chunk；
// - 不回放 WBL / exemplar / metadata / tombstones：对应 Append* 接口本身就是 no-op。
//
// 不恢复 open chunk 会不会丢数据？——不会：关机前已 commit 的样本都在 WAL 里，
// 下一次 flushWindow 从 WAL 流式读取窗口内样本喂给临时 RangeHead，最终落成 block。
// 启动后的 lastTs 用来阻挡乱序样本。
func (db *DB) replayWAL() error {
	start := time.Now()

	dir, startFrom, err := wlog.LastCheckpoint(db.wal.Dir())
	if err != nil && !errors.Is(err, record.ErrNotFound) {
		return errors.Wrap(err, "find last checkpoint")
	}

	if err == nil {
		sr, err := wlog.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		if err := db.loadWALSegments(wlog.NewReader(sr)); err != nil {
			sr.Close()
			return errors.Wrap(err, "replay checkpoint")
		}
		sr.Close()
		startFrom++
		level.Info(db.logger).Log("msg", "WAL checkpoint loaded")
	}

	_, last, err := wlog.Segments(db.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "find WAL segments")
	}
	for i := startFrom; i <= last; i++ {
		seg, err := wlog.OpenReadSegment(wlog.SegmentName(db.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment %d", i))
		}
		sr := wlog.NewSegmentBufReader(seg)
		if err := db.loadWALSegments(wlog.NewReader(sr)); err != nil {
			sr.Close()
			return err
		}
		sr.Close()
		level.Info(db.logger).Log("msg", "WAL segment loaded", "segment", i, "maxSegment", last)
	}

	db.metrics.walReplayDuration.Set(time.Since(start).Seconds())
	return nil
}

// loadWALSegments 解码一个 reader 里的所有记录，执行 replay 副作用。
//
// 只处理 Series 与 Samples。其它记录类型（Tombstones / Exemplars / Metadata /
// HistogramSamples / FloatHistogramSamples / MmapMarkers 等）在 litehead 里
// 当前都是 no-op，故 replay 阶段直接跳过，避免误更新 lastTs。
func (db *DB) loadWALSegments(r *wlog.Reader) error {
	var (
		dec     record.Decoder
		lastRef atomic.Uint64
	)
	lastRef.Store(db.nextRef.Load())

	series := make([]record.RefSeries, 0, 128)
	samples := make([]record.RefSample, 0, 1024)

	for r.Next() {
		rec := r.Record()
		switch dec.Type(rec) {
		case record.Series:
			var err error
			series, err = dec.Series(rec, series[:0])
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode series"), Segment: r.Segment(), Offset: r.Offset()}
			}
			for _, s := range series {
				// 若 ref 在 WAL 里重复出现（例如 checkpoint 后重新写过），
				// 保持先出现的那份；refTable 由 createSeriesWithRef 控制。
				if db.refTab.get(s.Ref) != nil {
					continue
				}
				db.createSeriesWithRef(s.Ref, s.Labels)
				if uint64(s.Ref) > lastRef.Load() {
					lastRef.Store(uint64(s.Ref))
				}
			}
		case record.Samples:
			var err error
			samples, err = dec.Samples(rec, samples[:0])
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode samples"), Segment: r.Segment(), Offset: r.Offset()}
			}
			for _, s := range samples {
				ws := db.refTab.get(s.Ref)
				if ws == nil {
					// series 可能已经被 GC / checkpoint 剔除；跳过即可。
					continue
				}
				if s.T > ws.lastTs {
					ws.lastTs = s.T
				}
				db.updateMinMaxTime(s.T)
			}
		default:
			// Tombstones / Exemplars / Metadata / HistogramSamples /
			// FloatHistogramSamples / MmapMarkers / 未知类型：litehead 不使用。
			continue
		}
	}
	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read WAL records")
	}
	if v := lastRef.Load(); v > db.nextRef.Load() {
		db.nextRef.Store(v)
	}
	return nil
}

// createSeriesWithRef 使用已知 ref 恢复 series。
// 与 createSeries 的区别：不分配新 ref，也不触发 seriesCreated 统计。
func (db *DB) createSeriesWithRef(ref chunks.HeadSeriesRef, lset labels.Labels) {
	if db.refTab.get(ref) != nil {
		return
	}
	labelsID := db.labelCat.put(lset)

	s := db.seriesPool.Get().(*memSeries)
	*s = memSeries{
		ref:      ref,
		labelsID: labelsID,
		lastTs:   math.MinInt64,
	}
	db.refTab.set(ref, s)
	db.hashIdx.put(lset.Hash(), ref, labelsID)
	db.metrics.seriesActive.Inc()
}
