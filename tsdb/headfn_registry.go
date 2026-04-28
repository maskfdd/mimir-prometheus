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
	"sync"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

// NewHeadFnType is the function signature for creating a HeadLike implementation.
type NewHeadFnType = func(l log.Logger, r prometheus.Registerer, dir string, opts *Options, rngs []int64) (HeadLike, error)

var (
	headFnMu       sync.RWMutex
	headFnRegistry = map[string]NewHeadFnType{}
)

// RegisterNewHeadFn registers a named HeadLike factory function.
// This is intended to be called from init() in sub-packages (e.g. litehead)
// to break the circular dependency: tsdb -> litehead -> tsdb.
//
// Example usage in litehead/factory.go:
//
//	func init() {
//	    tsdb.RegisterNewHeadFn("litehead", newLiteHeadFactory())
//	}
func RegisterNewHeadFn(name string, fn NewHeadFnType) {
	headFnMu.Lock()
	defer headFnMu.Unlock()
	headFnRegistry[name] = fn
}

// GetNewHeadFn returns a previously registered HeadLike factory by name.
// Returns nil if no factory with that name has been registered.
func GetNewHeadFn(name string) NewHeadFnType {
	headFnMu.RLock()
	defer headFnMu.RUnlock()
	return headFnRegistry[name]
}
