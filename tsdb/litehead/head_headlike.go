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

package litehead

// This file contains the HeadLike interface implementation for litehead.Head.
// Methods that are not applicable to a write-only head are implemented as no-ops
// or return sensible defaults.

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// Compile-time check: *Head implements tsdb.HeadLike.
var _ tsdb.HeadLike = (*Head)(nil)

// --- OOO time bounds (not supported) ---

// MinOOOTime returns math.MaxInt64 because litehead does not support out-of-order data.
func (h *Head) MinOOOTime() int64 { return math.MaxInt64 }

// MaxOOOTime returns math.MinInt64 because litehead does not support out-of-order data.
func (h *Head) MaxOOOTime() int64 { return math.MinInt64 }

// --- Truncation ---

// TruncateWAL delegates to the internal truncateWAL.
func (h *Head) TruncateWAL(mint int64) error {
	return h.truncateWAL(mint)
}

// TruncateMemory is a no-op for litehead. Memory management is handled by Flush.
func (h *Head) TruncateMemory(_ int64) error { return nil }

// Truncate removes old data before mint from both memory and WAL.
// For litehead this only truncates the WAL; in-memory data is managed by Flush.
func (h *Head) Truncate(mint int64) error {
	// DB.Open calls reload() before Init(). At that point WAL replay has not
	// rebuilt refTab/hashIdx yet, so checkpoint/truncate would be unsafe.
	// Init(minValidTime) will skip replaying samples already covered by blocks.
	if !h.initialized.Load() {
		return nil
	}
	return h.truncateWAL(mint)
}

// TruncateOOO is a no-op because litehead does not support out-of-order data.
func (h *Head) TruncateOOO(_ int, _ chunks.ChunkDiskMapperRef) error { return nil }

// SelfCompact flushes all in-memory data to on-disk blocks and truncates the
// WAL. It always returns handled=true so that DB.Compact() skips the standard
// RangeBlockReader + compactor.Write head-compact loop (which litehead does
// not support, because RangeBlockReader returns nil).
//
// The ctx is accepted for interface symmetry but not propagated: the
// underlying tryFlushAll path constructs its own context for the LeveledCompactor.
func (h *Head) SelfCompact(_ context.Context) (bool, error) {
	h.appenderMtx.Lock()
	defer h.appenderMtx.Unlock()

	if !h.compactable() {
		return true, nil
	}
	if err := h.tryFlushAll(); err != nil {
		return true, err
	}
	return true, nil
}

// --- Mmap / Isolation ---

// MmapHeadChunks is a no-op for litehead. Chunks are mmapped immediately on seal.
func (h *Head) MmapHeadChunks() {}

// WaitForAppendersOverlapping is a no-op because litehead has no isolation tracking.
func (h *Head) WaitForAppendersOverlapping(_ int64) {}

// --- Query support ---

// IsQuerierCollidingWithTruncation always returns no collision because litehead
// does not support queries.
func (h *Head) IsQuerierCollidingWithTruncation(_, _ int64) (shouldClose, getNew bool, newMint int64) {
	return false, false, 0
}

// OverlapsClosedInterval returns true if the head overlaps [mint, maxt].
func (h *Head) OverlapsClosedInterval(mint, maxt int64) bool {
	return h.MinTime() <= maxt && mint <= h.MaxTime()
}

// Delete is a no-op because litehead is write-only and does not support deletions.
func (h *Head) Delete(_ context.Context, _, _ int64, _ ...*labels.Matcher) error { return nil }

// --- Configuration ---

// ApplyConfig is a no-op because litehead does not support OOO/WBL configuration.
func (h *Head) ApplyConfig(_ *config.Config, _ *wlog.WL) {}

// EnableNativeHistograms is a no-op for litehead (native histograms not yet supported).
func (h *Head) EnableNativeHistograms() {}

// DisableNativeHistograms is a no-op for litehead.
func (h *Head) DisableNativeHistograms() {}

// SetWriteNotified is a no-op because litehead does not use write notifications.
func (h *Head) SetWriteNotified(_ wlog.WriteNotified) {}

// --- Metrics / Info ---

// IncrementWALCorruptionsTotal increments the WAL corruptions counter.
func (h *Head) IncrementWALCorruptionsTotal() {
	h.metrics.walCorruptionsTotal.Inc()
}

// WBL returns nil because litehead does not support out-of-order write-behind log.
func (h *Head) WBL() *wlog.WL { return nil }

// --- Block reader support ---

// RangeBlockReader is not supported by litehead. Queries should read flushed blocks.
// Returns nil; callers should check before using.
func (h *Head) RangeBlockReader(_, _ int64) tsdb.BlockReader { return nil }

// RangeBlockReaderWithIsolationDisabled is not supported by litehead.
// Returns nil; callers should check before using.
func (h *Head) RangeBlockReaderWithIsolationDisabled(_, _ int64) tsdb.BlockReader { return nil }

// OOORangeBlockReader is not supported because litehead has no OOO data.
func (h *Head) OOORangeBlockReader(_, _ int64) tsdb.BlockReader { return nil }

// NewOOOCompactionHead returns (nil, nil) because litehead has no OOO data.
func (h *Head) NewOOOCompactionHead(_ context.Context) (*tsdb.OOOCompactionHead, error) {
	return nil, nil
}
