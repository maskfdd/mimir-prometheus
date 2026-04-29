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

import (
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/tsdb"
)

func init() {
	// Auto-register the litehead factory so that tsdb.Options.UseLiteHead works
	// with just a blank import:
	//
	//   import _ "github.com/prometheus/prometheus/tsdb/litehead"
	tsdb.RegisterNewHeadFn("litehead", newLiteHeadFactory())
}

// newLiteHeadFactory returns the internal factory function that maps
// tsdb.Options to litehead.Options and creates a litehead.Head.
func newLiteHeadFactory() tsdb.NewHeadFnType {
	return func(l log.Logger, r prometheus.Registerer, dir string, opts *tsdb.Options, rngs []int64) (tsdb.HeadLike, error) {
		liteOpts := &Options{
			ChunkRange:                     rngs[0],
			BlockDuration:                  rngs[0],
			ChunkWriteBufferSize:           opts.HeadChunksWriteBufferSize,
			ChunkWriteQueueSize:            opts.HeadChunksWriteQueueSize,
			SamplesPerChunk:                opts.SamplesPerChunk,
			EnableMemorySnapshotOnShutdown: opts.EnableMemorySnapshotOnShutdown,
			WALSegmentSize:                 opts.WALSegmentSize,
			WALCompression:                 opts.WALCompression,
			WALReplayConcurrency:           opts.WALReplayConcurrency,
			NoLockfile:                     true, // DB already handles directory locking.
			SeriesLifecycleCallback:        opts.SeriesLifecycleCallback,

			// litehead-specific tunables. Options.validate() falls back to
			// package defaults when any of these are <= 0.
			ForcedFlushSealedChunks: opts.LiteHeadForcedFlushSealedChunks,
			SoftFlushSealedChunks:   opts.LiteHeadSoftFlushSealedChunks,
			FlushCheckInterval:      opts.LiteHeadFlushCheckInterval,
		}
		return NewHead(l, r, dir, liteOpts)
	}
}

// NewLiteHeadFn returns a factory function compatible with tsdb.Options.NewHeadFn.
// It creates a litehead.Head from the DB-level Options, mapping the relevant fields
// automatically so that the caller only needs:
//
//	db, err := tsdb.Open(dir, logger, reg, &tsdb.Options{
//	    ...
//	    NewHeadFn: litehead.NewLiteHeadFn(),
//	}, nil)
//
// Prefer using tsdb.Options.UseLiteHead = true with a blank import of this package
// for an even simpler integration.
func NewLiteHeadFn() tsdb.NewHeadFnType {
	return newLiteHeadFactory()
}
