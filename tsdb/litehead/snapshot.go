package litehead

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
//
// # 并行化（PR-5）
//
// 老实现串行遍历全表：`forEach -> encode -> 累到 buf -> 达到 10MB flush`。大库下
// encode + `labelCat.get` 解码是主要耗时点。这里改成 **按 refPage 分 worker 并行
// 编码，主 goroutine 串行 cp.Log**：
//
//  1. `snapshotPages()` 短时间持 RLock 拿 pages 浅拷贝；
//  2. worker 并行编码各自分配到的 pages，产出独立的 `[][]byte` 记录列表（每 ~10MB
//     切一段，以降低记录 batch 的内存峰值）；
//  3. 主 goroutine 按任意顺序串行写入 cp.Log —— snapshot 加载时也不依赖顺序。
//
// 注意：
//   - `h.labelCat.get` 是并发安全的（内部 RLock）。
//   - writeSnapshot 通常只在 Close 路径被调用，此时写已停止，`refPage.entries`
//     的槽位不会再变；即便在极端场景下存在并发 set/del，worker 侧的 nil 判断
//     与只读 `*memSeries` 字段已足够安全（不并发修改 lastTs / labelsID / ref）。
//   - Worker 的错误通过 `errCh` 汇聚，主 goroutine 在遇到首个错误时尽快返回；
//     已写入的临时目录会被 defer 清理。
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

	// snapshotEncodeFlushBytes 是单个 worker 累积到此阈值时产出一个 batch 给
	// 主 goroutine 去 cp.Log。保持与老实现 10MB flush 一致，避免 batch 过大
	// 占用内存、过小引起 Log 开销放大。
	const snapshotEncodeFlushBytes = 10 * 1024 * 1024

	pages := h.refTab.snapshotPages()
	workerCount := h.snapshotWorkerCount(len(pages))

	// batchCh: worker -> main，`[][]byte` 批记录
	// errCh:   worker -> main，首个错误
	type batch struct {
		buf  []byte   // 整块字节缓冲；保证 records 切片指向的都是此 buf 的 sub-slice
		recs [][]byte // records 引用 buf 的区间
	}
	batchCh := make(chan batch, workerCount*2)
	errCh := make(chan error, workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	// 轮询分配 pages 给 worker：page i 交给 worker (i % workerCount)。
	// 这样做比"均分连续段"更抗倾斜：单个 page 最多 refPageSize(1<<14)=16384 条，
	// 实际写入分布上 page 饱和度差异大，轮询让各 worker 的活跃 entry 数更均匀。
	for w := 0; w < workerCount; w++ {
		go func(workerID int) {
			defer wg.Done()
			var (
				buf  []byte
				recs [][]byte
			)
			flush := func() {
				if len(recs) == 0 {
					return
				}
				// 把当前 batch 交出去；自己起新的底层 buf，避免后续 append 影响
				// 已交出的 records（append 到 full-cap 时会替换底层数组，会污染）。
				select {
				case batchCh <- batch{buf: buf, recs: recs}:
				case <-errCh:
					// 已有错误发生，不再入队。
				}
				buf, recs = nil, nil
			}

			for i := workerID; i < len(pages); i += workerCount {
				p := pages[i]
				if p == nil {
					continue
				}
				for _, s := range p.entries {
					if s == nil {
						continue
					}
					// 读 series 的只读字段（ref / labelsID / lastTs）；labelCat.get 内部自锁。
					lset := h.labelCat.get(s.labelsID)
					start := len(buf)
					buf = encodeLiteSnapshotRecord(buf, s.ref, lset, s.lastTs)
					recs = append(recs, buf[start:])

					if len(buf) >= snapshotEncodeFlushBytes {
						flush()
					}
				}
			}
			flush()
		}(w)
	}

	// 主 goroutine：串行 cp.Log，统计 numSeries；遇错中止。
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(batchCh)
		close(done)
	}()

	numSeries := 0
	var logErr error
	for b := range batchCh {
		if logErr != nil {
			continue // 已出错，drain 避免 goroutine 阻塞。
		}
		if err := cp.Log(b.recs...); err != nil {
			logErr = errors.Wrap(err, "flush lite snapshot records")
			// 通知 worker 尽快停。
			select {
			case errCh <- logErr:
			default:
			}
			continue
		}
		numSeries += len(b.recs)
	}
	<-done

	if logErr != nil {
		return logErr
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

	level.Info(h.logger).Log("msg", "lite snapshot created", "dir", snapshotName,
		"num_series", numSeries, "workers", workerCount)
	return nil
}

// snapshotWorkerCount 选择 snapshot 的并行度。优先用 Options.WALReplayConcurrency
// 作为上限；否则按 GOMAXPROCS 的一半（最少 1，最多 8）。对规模很小（pages<=2）
// 的 Head 直接走串行（1 worker），避免并行开销反而更慢。
func (h *Head) snapshotWorkerCount(pageCount int) int {
	if pageCount <= 2 {
		return 1
	}
	if h.opts.WALReplayConcurrency > 0 {
		n := h.opts.WALReplayConcurrency
		if n > pageCount {
			n = pageCount
		}
		return n
	}
	n := runtime.GOMAXPROCS(0) / 2
	if n < 1 {
		n = 1
	}
	if n > 8 {
		n = 8
	}
	if n > pageCount {
		n = pageCount
	}
	return n
}

// ──────── 快照加载 ────────

// loadSnapshot 加载最新快照，恢复 refTab/hashIdx/labelCat。
// 返回快照对应的 WAL segment 索引和 offset；无快照时返回 (-1, -1, nil)。
//
// # 并行化（PR-5）
//
// 老实现：**串行** WAL reader 逐条 decode + 立刻 createSeriesWithRef。decode 会
// 解析 labels + `labelCat.put` + `hashIdx.put` + `refTab.set`，单条延迟低但累加起来
// 是大库重启的主要耗时。
//
// 这里把管道拆成三段：
//   - **串行**从 WAL reader 读出 raw bytes（reader 非并发安全）
//   - **并行** worker 把 raw bytes decode 成 (ref, lset, lastTs)
//   - **串行**主 goroutine 按产出顺序调用 createSeriesWithRef（createSeriesWithRef
//     需要串行：它同时修改 refTab / hashIdx / labelCat / lastSeriesID / numSeries）
//
// 为保持 snapshot 老行为的可观察性（createSeriesWithRef 的顺序），records 通过带索引
// 的管道回流——但由于 snapshot 的语义只要求"最后状态等价"，即便乱序也正确；采用
// 顺序回流主要是为了让 maxRef / updateMinMaxTime 的观察更稳定。
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

	// 估计 record 量：没有总数元数据，按 4096 起步即可。
	type decoded struct {
		ref    chunks.HeadSeriesRef
		lset   labels.Labels
		lastTs int64
		err    error
	}

	workerCount := h.snapshotWorkerCount(4) // 当前没有 page 总数信息，按默认并行度估算
	// 小规模 snapshot（<= 64 条）不值得开并行。阈值通过读几条 raw 后动态判断：
	// 这里采用"先把 raw bytes 全读到内存"的形态 —— 与老实现空间复杂度同级，
	// 但内存峰值更大；这是工程折中。大规模 snapshot（>1M 条）场景下，单条
	// raw 平均几十字节，全量内存驻留在几十 MiB 量级，可接受。
	var rawRecs [][]byte
	for r.Next() {
		rec := r.Record()
		// 注意：Reader.Record() 返回的 slice 会在下次 Next() 时被覆盖，
		// 必须 copy 一份才能异步消费。
		cp := make([]byte, len(rec))
		copy(cp, rec)
		rawRecs = append(rawRecs, cp)
	}
	if r.Err() != nil {
		return -1, -1, errors.Wrap(r.Err(), "read lite snapshot records")
	}

	// 规模太小直接串行走完，避免并行开销。
	if len(rawRecs) <= 64 || workerCount <= 1 {
		for _, rec := range rawRecs {
			ref, lset, lastTs, decErr := decodeLiteSnapshotRecord(rec)
			if decErr != nil {
				return -1, -1, errors.Wrap(decErr, "decode lite snapshot record")
			}
			h.applySnapshotRecord(ref, lset, lastTs)
			numSeries++
		}
	} else {
		// 并行 decode：每个 worker 处理自己区段的 raw bytes，产出 decoded 记录。
		// 区段切分采用连续分片（contiguous），让各 worker 缓存局部性更好。
		segSize := (len(rawRecs) + workerCount - 1) / workerCount
		results := make([][]decoded, workerCount)
		var wg sync.WaitGroup
		wg.Add(workerCount)
		for w := 0; w < workerCount; w++ {
			lo := w * segSize
			hi := lo + segSize
			if hi > len(rawRecs) {
				hi = len(rawRecs)
			}
			go func(idx, lo, hi int) {
				defer wg.Done()
				if lo >= hi {
					return
				}
				out := make([]decoded, 0, hi-lo)
				for j := lo; j < hi; j++ {
					ref, lset, lastTs, decErr := decodeLiteSnapshotRecord(rawRecs[j])
					if decErr != nil {
						out = append(out, decoded{err: errors.Wrap(decErr, "decode lite snapshot record")})
						continue
					}
					out = append(out, decoded{ref: ref, lset: lset, lastTs: lastTs})
				}
				results[idx] = out
			}(w, lo, hi)
		}
		wg.Wait()

		// 串行合并：按 worker 编号顺序 apply，保证观察到的 maxRef / minMax 单调演进。
		for _, out := range results {
			for _, d := range out {
				if d.err != nil {
					return -1, -1, d.err
				}
				h.applySnapshotRecord(d.ref, d.lset, d.lastTs)
				numSeries++
			}
		}
	}

	elapsed := time.Since(start)
	level.Info(h.logger).Log("msg", "lite snapshot loaded", "dir", dir,
		"num_series", numSeries, "duration", elapsed, "workers", workerCount)
	h.metrics.snapshotLoadDuration.Set(elapsed.Seconds())
	return snapIdx, snapOffset, nil
}

// applySnapshotRecord 把一条已 decode 的 snapshot 记录合并到 Head 主结构。
// 封装原 loadSnapshot 主循环中的 3 个步骤，便于串行路径和并行路径共用。
func (h *Head) applySnapshotRecord(ref chunks.HeadSeriesRef, lset labels.Labels, lastTs int64) {
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
}
