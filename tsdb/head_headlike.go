// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"

	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// This file contains public wrapper methods on the standard Head that expose
// private methods, so that Head satisfies the HeadLike interface defined in
// headlike.go. These wrappers exist purely to bridge the interface gap without
// renaming the original private methods (which would be a much larger change).

// Compile-time check: *Head implements HeadLike.
var _ HeadLike = (*Head)(nil)

// IsCompactable returns whether the head has a compactable range.
// It wraps the private compactable() method.
func (h *Head) IsCompactable() bool {
	return h.compactable()
}

// TruncateWAL removes old data before mint from the WAL.
// It wraps the private truncateWAL() method.
func (h *Head) TruncateWAL(mint int64) error {
	return h.truncateWAL(mint)
}

// TruncateMemory removes old data before mint from memory.
// It wraps the private truncateMemory() method.
func (h *Head) TruncateMemory(mint int64) error {
	return h.truncateMemory(mint)
}

// TruncateOOO truncates out-of-order data.
// It wraps the private truncateOOO() method.
func (h *Head) TruncateOOO(lastWBLFile int, minOOOMmapRef chunks.ChunkDiskMapperRef) error {
	return h.truncateOOO(lastWBLFile, minOOOMmapRef)
}

// MmapHeadChunks triggers mmap of in-memory head chunks.
// It wraps the private mmapHeadChunks() method.
func (h *Head) MmapHeadChunks() {
	h.mmapHeadChunks()
}

// SetWriteNotified sets the write notification callback.
func (h *Head) SetWriteNotified(wn wlog.WriteNotified) {
	h.writeNotified = wn
}

// IncrementWALCorruptionsTotal increments the WAL corruptions counter.
func (h *Head) IncrementWALCorruptionsTotal() {
	h.metrics.walCorruptionsTotal.Inc()
}

// WBL returns the out-of-order write-ahead log, or nil if not available.
func (h *Head) WBL() *wlog.WL {
	return h.wbl
}

// ChunkRange returns the configured chunk time range in milliseconds.
func (h *Head) ChunkRange() int64 {
	return h.chunkRange.Load()
}

// RangeBlockReader returns a BlockReader for the given time range.
// This wraps NewRangeHead(head, mint, maxt).
func (h *Head) RangeBlockReader(mint, maxt int64) BlockReader {
	return NewRangeHead(h, mint, maxt)
}

// RangeBlockReaderWithIsolationDisabled returns a BlockReader with isolation disabled.
// This wraps NewRangeHeadWithIsolationDisabled(head, mint, maxt).
func (h *Head) RangeBlockReaderWithIsolationDisabled(mint, maxt int64) BlockReader {
	return NewRangeHeadWithIsolationDisabled(h, mint, maxt)
}

// OOORangeBlockReader returns a BlockReader for out-of-order data.
// This wraps NewOOORangeHead(head, mint, maxt).
func (h *Head) OOORangeBlockReader(mint, maxt int64) BlockReader {
	return NewOOORangeHead(h, mint, maxt)
}

// NewOOOCompactionHead creates an OOOCompactionHead for OOO compaction.
// This wraps the package-level NewOOOCompactionHead(ctx, head).
func (h *Head) NewOOOCompactionHead(ctx context.Context) (*OOOCompactionHead, error) {
	return NewOOOCompactionHead(ctx, h)
}
