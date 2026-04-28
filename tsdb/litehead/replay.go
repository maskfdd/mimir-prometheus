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

// replayChunkDiskMapper 初始化 mmap 文件并恢复最大 HeadSeriesRef。
func (h *Head) replayChunkDiskMapper() error {
	var maxRef chunks.HeadSeriesRef
	iterFn := func(seriesRef chunks.HeadSeriesRef, _ chunks.ChunkDiskMapperRef, _, _ int64, _ uint16, _ chunkenc.Encoding, _ bool) error {
		if seriesRef > maxRef {
			maxRef = seriesRef
		}
		return nil
	}
	err := h.chunkDiskMapper.IterateAllChunks(iterFn)
	if err != nil {
		return errors.Wrap(err, "iterate chunk disk mapper")
	}
	// 兜底更新 lastSeriesID，后续 WAL replay 还会继续更新。
	if uint64(maxRef) > h.lastSeriesID.Load() {
		h.lastSeriesID.Store(uint64(maxRef))
	}
	return nil
}

// replayWAL 从 snapshot（如有） + checkpoint + WAL 段恢复状态。
//
// 只恢复 refTable/hashIndex/labelCatalog 映射、每条 series 的 lastTs，
// 以及 minT/maxT。不重建倒排索引、open chunk、WBL、exemplar 等。
func (h *Head) replayWAL() error {
	start := time.Now()

	// 尝试加载 snapshot。
	snapIdx, snapOffset := -1, -1
	if h.opts.EnableMemorySnapshotOnShutdown {
		var err error
		snapIdx, snapOffset, err = h.loadSnapshot()
		if err != nil {
			level.Warn(h.logger).Log("msg", "snapshot load failed, falling back to full WAL replay", "err", err)
			snapIdx, snapOffset = -1, -1
		}
	}

	dir, startFrom, err := wlog.LastCheckpoint(h.wal.Dir())
	if err != nil && !errors.Is(err, record.ErrNotFound) {
		return errors.Wrap(err, "find last checkpoint")
	}

	// 确定 replay 起点：取 snapshot 和 checkpoint 位置的较大者。
	replayCheckpoint := true
	if snapIdx >= 0 {
		if err == nil && startFrom <= snapIdx {
			replayCheckpoint = false
		} else if errors.Is(err, record.ErrNotFound) {
			replayCheckpoint = false
		}
	}

	if replayCheckpoint && err == nil {
		sr, err := wlog.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		if err := h.loadWALSegments(wlog.NewReader(sr)); err != nil {
			sr.Close()
			return errors.Wrap(err, "replay checkpoint")
		}
		sr.Close()
		startFrom++
		level.Info(h.logger).Log("msg", "WAL checkpoint loaded")
	} else if err == nil {
		startFrom++
	}

	// 如果 snapshot 比 checkpoint 更新，从 snapshot 位置开始。
	walStartFrom := startFrom
	walStartOffset := 0
	if snapIdx >= 0 && snapIdx >= startFrom {
		walStartFrom = snapIdx
		walStartOffset = snapOffset
	}

	_, last, err := wlog.Segments(h.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "find WAL segments")
	}
	for i := walStartFrom; i <= last; i++ {
		seg, err := wlog.OpenReadSegment(wlog.SegmentName(h.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment %d", i))
		}
		if i == snapIdx && walStartOffset > 0 {
			// 从 snapshot 记录的 offset 开始读。
			sr, err := wlog.NewSegmentBufReaderWithOffset(walStartOffset, seg)
			if err != nil {
				seg.Close()
				return errors.Wrap(err, fmt.Sprintf("create offset reader for WAL segment %d", i))
			}
			if err := h.loadWALSegments(wlog.NewReader(sr)); err != nil {
				sr.Close()
				return err
			}
			sr.Close()
		} else {
			sr := wlog.NewSegmentBufReader(seg)
			if err := h.loadWALSegments(wlog.NewReader(sr)); err != nil {
				sr.Close()
				return err
			}
			sr.Close()
		}
		level.Info(h.logger).Log("msg", "WAL segment loaded", "segment", i, "maxSegment", last)
	}

	h.metrics.walReplayDuration.Set(time.Since(start).Seconds())
	return nil
}

// loadWALSegments 解码 reader 中的所有记录，只处理 Series 和 Samples。
func (h *Head) loadWALSegments(r *wlog.Reader) error {
	var (
		dec          record.Decoder
		lastRef      atomic.Uint64
		minValidTime = h.minValidTime.Load()
	)
	lastRef.Store(h.lastSeriesID.Load())

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
				if h.refTab.get(s.Ref) != nil {
					continue
				}
				h.createSeriesWithRef(s.Ref, s.Labels)
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
				if s.T < minValidTime {
					continue
				}
				ws := h.refTab.get(s.Ref)
				if ws == nil {
					continue
				}
				if s.T > ws.lastTs {
					ws.lastTs = s.T
				}
				h.updateMinMaxTime(s.T)
			}
		default:
			continue
		}
	}
	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read WAL records")
	}
	if v := lastRef.Load(); v > h.lastSeriesID.Load() {
		h.lastSeriesID.Store(v)
	}
	return nil
}

// createSeriesWithRef 使用已知 ref 恢复 series（WAL replay 路径）。
func (h *Head) createSeriesWithRef(ref chunks.HeadSeriesRef, lset labels.Labels) {
	if h.refTab.get(ref) != nil {
		return
	}
	labelsID := h.labelCat.put(lset)

	s := h.seriesPool.Get().(*memSeries)
	*s = memSeries{
		ref:      ref,
		labelsID: labelsID,
		lastTs:   math.MinInt64,
	}
	h.refTab.set(ref, s)
	h.hashIdx.put(lset.Hash(), ref, labelsID)
	h.numSeries.Inc()
	h.metrics.seriesActive.Inc()
}
