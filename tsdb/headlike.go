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

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// HeadLike is the interface that DB uses to interact with the Head block.
// Both the standard Head and litehead.Head implement this interface.
// This allows DB to be agnostic about which Head implementation is used.
type HeadLike interface {
	// --- Lifecycle ---

	// Init initializes the head by replaying WAL/snapshot data.
	// minValidTime sets the lower bound for valid sample timestamps.
	Init(minValidTime int64) error

	// Close flushes pending data and releases all resources.
	Close() error

	// --- Time bounds ---

	MinTime() int64
	MaxTime() int64

	// MinOOOTime returns the minimum out-of-order time.
	// Implementations without OOO support should return math.MaxInt64.
	MinOOOTime() int64

	// MaxOOOTime returns the maximum out-of-order time.
	// Implementations without OOO support should return math.MinInt64.
	MaxOOOTime() int64

	// --- Append ---

	Appender(ctx context.Context) storage.Appender

	// --- Compaction ---

	// IsCompactable returns whether the head has a compactable range.
	IsCompactable() bool

	// ChunkRange returns the configured chunk time range in milliseconds.
	ChunkRange() int64

	// TruncateWAL removes old data before mint from the WAL.
	TruncateWAL(mint int64) error

	// TruncateMemory removes old data before mint from memory.
	TruncateMemory(mint int64) error

	// Truncate removes old data before mint from both memory and WAL.
	Truncate(mint int64) error

	// TruncateOOO truncates out-of-order data.
	// Implementations without OOO support should return nil.
	TruncateOOO(lastWBLFile int, minOOOMmapRef chunks.ChunkDiskMapperRef) error

	// MmapHeadChunks triggers mmap of in-memory head chunks.
	// Implementations that mmap immediately (like litehead) can make this a no-op.
	MmapHeadChunks()

	// WaitForAppendersOverlapping waits for in-flight appenders that overlap maxt.
	// Implementations without isolation can make this a no-op.
	WaitForAppendersOverlapping(maxt int64)

	// --- Query support (used by DB for query routing) ---

	// IsQuerierCollidingWithTruncation checks if a querier collides with an in-progress truncation.
	// Implementations without truncation collision tracking should return (false, false, 0).
	IsQuerierCollidingWithTruncation(querierMint, querierMaxt int64) (shouldClose, getNew bool, newMint int64)

	// OverlapsClosedInterval returns true if the head overlaps [mint, maxt].
	OverlapsClosedInterval(mint, maxt int64) bool

	// Delete removes samples in [mint, maxt] for series matching the given matchers.
	// Implementations without delete support should return nil.
	Delete(ctx context.Context, mint, maxt int64, ms ...*labels.Matcher) error

	// --- Configuration ---

	// ApplyConfig applies a new configuration to the head.
	// Implementations that don't support OOO/WBL can make this a no-op.
	ApplyConfig(cfg *config.Config, wbl *wlog.WL)

	// EnableNativeHistograms enables native histogram ingestion.
	EnableNativeHistograms()

	// DisableNativeHistograms disables native histogram ingestion.
	DisableNativeHistograms()

	// SetWriteNotified sets the write notification callback.
	SetWriteNotified(wn wlog.WriteNotified)

	// --- Metrics / Info ---

	Size() int64

	// NumSeries returns the number of active series in the head.
	NumSeries() uint64

	// Stats returns important current HEAD statistics.
	Stats(statsByLabelName string, limit int) *Stats

	// IncrementWALCorruptionsTotal increments the WAL corruptions counter.
	IncrementWALCorruptionsTotal()

	// WBL returns the out-of-order write-ahead log, or nil if not available.
	WBL() *wlog.WL

	// ExemplarQuerier returns an ExemplarQuerier for the head.
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

	// --- Block reader support (for compaction) ---

	// RangeBlockReader returns a BlockReader for the given time range.
	// This replaces the direct NewRangeHead(head, mint, maxt) pattern.
	RangeBlockReader(mint, maxt int64) BlockReader

	// RangeBlockReaderWithIsolationDisabled returns a BlockReader with isolation disabled.
	// This replaces NewRangeHeadWithIsolationDisabled(head, mint, maxt).
	RangeBlockReaderWithIsolationDisabled(mint, maxt int64) BlockReader

	// OOORangeBlockReader returns a BlockReader for out-of-order data.
	// Implementations without OOO support should return nil.
	OOORangeBlockReader(mint, maxt int64) BlockReader

	// NewOOOCompactionHead creates an OOOCompactionHead for OOO compaction.
	// Implementations without OOO support should return (nil, nil).
	NewOOOCompactionHead(ctx context.Context) (*OOOCompactionHead, error)
}
