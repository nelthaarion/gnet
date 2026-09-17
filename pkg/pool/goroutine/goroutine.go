// Copyright (c) 2019 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package goroutine is a wrapper of github.com/panjf2000/ants with some practical configurations.
package goroutine

import (
	"time"

	"github.com/nelthaarion/ants/v2"

	"github.com/nelthaarion/gnet/v2/pkg/logging"
)

const (
	// DefaultAntsPoolSize sets up the capacity of worker pool, 256 * 1024.
	DefaultAntsPoolSize = 1 << 18

	// ExpiryDuration is the interval time to clean up those expired workers.
	ExpiryDuration = 10 * time.Second

	// Nonblocking decides what to do when submitting a new task to a full worker pool: waiting for a available worker
	// or returning nil directly.
	Nonblocking = true
)

func init() {
	// It releases the default pool from ants.
	ants.Release()
}

// DefaultWorkerPool is the global worker pool.
var DefaultWorkerPool = Default()

// Pool is the alias of ants.Pool.
type Pool = ants.Pool

// antsLogger adapts the logging package to the ants.Logger interface.
//
// FIX L-11: it used to embed the logging.Logger that was installed when this
// package was initialised — which happens before gnet applies WithLogger() in
// createListeners. The pool therefore kept writing through the process default
// for its whole life and a user-supplied logger never saw a single line from it.
// Resolving the logger per call instead means the pool follows the same logger as
// the rest of the library, including a later SetDefaultLoggerAndFlusher.
type antsLogger struct{}

// Printf implements the ants.Logger interface.
func (antsLogger) Printf(format string, args ...any) {
	logging.Infof(format, args...)
}

// Default instantiates a non-blocking goroutine pool with the capacity of DefaultAntsPoolSize.
func Default() *Pool {
	options := ants.Options{
		ExpiryDuration: ExpiryDuration,
		Nonblocking:    Nonblocking,
		Logger:         antsLogger{},
		PanicHandler: func(a any) {
			logging.Errorf("goroutine pool panic: %v", a)
		},
	}
	defaultAntsPool, _ := ants.NewPool(DefaultAntsPoolSize, ants.WithOptions(options))
	return defaultAntsPool
}
