package litehead

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// liteSnapshotPrefix 是快照目录前缀，沿用标准 Head 的命名。
const liteSnapshotPrefix = "chunk_snapshot."

// liteSnapshotRecordTypeSeries 是快照记录的类型标识。
const liteSnapshotRecordTypeSeries uint8 = 1

// liteSnapshotDir 返回快照目录名：chunk_snapshot.NNNNNN.MMMMMMMMMM。
func liteSnapshotDir(wlast, woffset int) string {
	return fmt.Sprintf(liteSnapshotPrefix+"%06d.%010d", wlast, woffset)
}

// lastLiteSnapshot 返回 dir 中最新的 snapshot 目录路径、segment 索引和 offset。
func lastLiteSnapshot(dir string) (string, int, int, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", 0, 0, err
	}
	maxIdx, maxOffset := -1, -1
	maxFileName := ""
	for _, fi := range files {
		if !strings.HasPrefix(fi.Name(), liteSnapshotPrefix) {
			continue
		}
		if !fi.IsDir() {
			continue
		}
		// 忽略临时目录。
		if strings.HasSuffix(fi.Name(), ".tmp") {
			continue
		}
		splits := strings.Split(fi.Name()[len(liteSnapshotPrefix):], ".")
		if len(splits) != 2 {
			continue
		}
		idx, err := strconv.Atoi(splits[0])
		if err != nil {
			continue
		}
		offset, err := strconv.Atoi(splits[1])
		if err != nil {
			continue
		}
		if idx > maxIdx || (idx == maxIdx && offset > maxOffset) {
			maxIdx, maxOffset = idx, offset
			maxFileName = filepath.Join(dir, fi.Name())
		}
	}
	if maxFileName == "" {
		return "", 0, 0, record.ErrNotFound
	}
	return maxFileName, maxIdx, maxOffset, nil
}

// deleteLiteSnapshots 删除比 (maxIndex, maxOffset) 旧的快照目录。
func deleteLiteSnapshots(dir string, maxIndex, maxOffset int) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !strings.HasPrefix(fi.Name(), liteSnapshotPrefix) {
			continue
		}
		splits := strings.Split(fi.Name()[len(liteSnapshotPrefix):], ".")
		if len(splits) != 2 {
			continue
		}
		idx, err := strconv.Atoi(splits[0])
		if err != nil {
			continue
		}
		offset, err := strconv.Atoi(splits[1])
		if err != nil {
			continue
		}
		if idx < maxIndex || (idx == maxIndex && offset < maxOffset) {
			os.RemoveAll(filepath.Join(dir, fi.Name()))
		}
	}
	return nil
}

// ──────── record 编码/解码 ────────
//
// 快照记录格式：
//
//	[1B] recordType=1  [8B] ref  [变长] labels  [8B] lastTs

// encodeLiteSnapshotRecord 编码一条 series 快照记录。
func encodeLiteSnapshotRecord(b []byte, ref chunks.HeadSeriesRef, lset labels.Labels, lastTs int64) []byte {
	buf := encoding.Encbuf{B: b}
	buf.PutByte(liteSnapshotRecordTypeSeries)
	buf.PutBE64(uint64(ref))
	record.EncodeLabels(&buf, lset)
	buf.PutBE64int64(lastTs)
	return buf.Get()
}

// decodeLiteSnapshotRecord 解码一条快照记录。
func decodeLiteSnapshotRecord(b []byte) (chunks.HeadSeriesRef, labels.Labels, int64, error) {
	dec := encoding.Decbuf{B: b}

	if flag := dec.Byte(); flag != liteSnapshotRecordTypeSeries {
		return 0, labels.EmptyLabels(), 0, errors.Errorf("invalid lite snapshot record type %x", flag)
	}
	ref := chunks.HeadSeriesRef(dec.Be64())

	var d record.Decoder
	lset := d.DecodeLabels(&dec)

	lastTs := dec.Be64int64()

	if dec.Err() != nil {
		return 0, labels.EmptyLabels(), 0, dec.Err()
	}
	return ref, lset, lastTs, nil
}

// ──────── 快照写入 ────────

// writeSnapshot 遍历 refTable 把每条 series 的 (ref, labels, lastTs) 写入快照目录。
func (h *Head) writeSnapshot() error {
	wlast, woffset, err := h.wal.LastSegmentAndOffset()
	if err != nil && !errors.Is(err, record.ErrNotFound) {
		return errors.Wrap(err, "get last WAL segment and offset")
	}

	// 检查 WAL 是否有新数据。
	_, cslast, csoffset, csErr := lastLiteSnapshot(h.dir)
	if csErr == nil && wlast == cslast && woffset == csoffset {
		return nil
	}

	snapshotName := liteSnapshotDir(wlast, woffset)
	cpdir := filepath.Join(h.dir, snapshotName)
	cpdirtmp := cpdir + ".tmp"

	if err := os.MkdirAll(cpdirtmp, 0o777); err != nil {
		return errors.Wrap(err, "create lite snapshot dir")
	}

	cp, err := wlog.New(nil, nil, cpdirtmp, h.wal.CompressionType())
	if err != nil {
		return errors.Wrap(err, "open lite snapshot WAL")
	}

	// 出错时清理临时目录。
	defer func() {
		cp.Close()
		os.RemoveAll(cpdirtmp)
	}()

	var (
		buf  []byte
		recs [][]byte
	)

	numSeries := 0
	h.refTab.forEach(func(s *memSeries) {
		start := len(buf)
		lset := h.labelCat.get(s.labelsID)
		buf = encodeLiteSnapshotRecord(buf, s.ref, lset, s.lastTs)
		recs = append(recs, buf[start:])
		numSeries++

		// 每 10 MB flush 一次。
		if len(buf) > 10*1024*1024 {
			if flushErr := cp.Log(recs...); flushErr != nil {
				level.Warn(h.logger).Log("msg", "flush lite snapshot records", "err", flushErr)
			}
			buf, recs = buf[:0], recs[:0]
		}
	})

	// Flush 剩余 records。
	if len(recs) > 0 {
		if err := cp.Log(recs...); err != nil {
			return errors.Wrap(err, "flush remaining lite snapshot records")
		}
	}

	if err := cp.Close(); err != nil {
		return errors.Wrap(err, "close lite snapshot")
	}

	if err := fileutil.Replace(cpdirtmp, cpdir); err != nil {
		return errors.Wrap(err, "rename lite snapshot directory")
	}

	// 删除旧快照。
	if err := deleteLiteSnapshots(h.dir, wlast, woffset); err != nil {
		level.Warn(h.logger).Log("msg", "delete old lite snapshots", "err", err)
	}

	level.Info(h.logger).Log("msg", "lite snapshot created", "dir", snapshotName, "num_series", numSeries)
	return nil
}

// ──────── 快照加载 ────────

// loadSnapshot 加载最新快照，恢复 refTab/hashIdx/labelCat。
// 返回快照对应的 WAL segment 索引和 offset；无快照时返回 (-1, -1, nil)。
func (h *Head) loadSnapshot() (snapIdx int, snapOffset int, err error) {
	dir, snapIdx, snapOffset, err := lastLiteSnapshot(h.dir)
	if err != nil {
		if errors.Is(err, record.ErrNotFound) {
			return -1, -1, nil
		}
		return -1, -1, errors.Wrap(err, "find last lite snapshot")
	}

	start := time.Now()
	sr, err := wlog.NewSegmentsReader(dir)
	if err != nil {
		return -1, -1, errors.Wrap(err, "open lite snapshot")
	}
	defer sr.Close()

	r := wlog.NewReader(sr)
	numSeries := 0

	for r.Next() {
		rec := r.Record()
		ref, lset, lastTs, decErr := decodeLiteSnapshotRecord(rec)
		if decErr != nil {
			return -1, -1, errors.Wrap(decErr, "decode lite snapshot record")
		}

		h.createSeriesWithRef(ref, lset)

		ws := h.refTab.get(ref)
		if ws != nil {
			ws.lastTs = lastTs
			if lastTs != math.MinInt64 {
				h.updateMinMaxTime(lastTs)
			}
		}

		if uint64(ref) > h.lastSeriesID.Load() {
			h.lastSeriesID.Store(uint64(ref))
		}

		numSeries++
	}
	if r.Err() != nil {
		return -1, -1, errors.Wrap(r.Err(), "read lite snapshot records")
	}

	elapsed := time.Since(start)
	level.Info(h.logger).Log("msg", "lite snapshot loaded", "dir", dir, "num_series", numSeries, "duration", elapsed)
	h.metrics.snapshotLoadDuration.Set(elapsed.Seconds())
	return snapIdx, snapOffset, nil
}
