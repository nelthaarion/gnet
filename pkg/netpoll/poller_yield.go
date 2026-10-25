// Copyright (c) 2025 The Gnet Authors. All rights reserved.
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

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package netpoll

// pollYield holds the poller's scheduler-yield policy; it is embedded in every
// Poller implementation so they all share one definition of it.
//
// By default a poller that finds nothing to do calls runtime.Gosched before it
// blocks in the kernel, so goroutines that the event callbacks just made runnable
// (worker pools, AsyncWrite producers, ...) get to run on this P straight away
// instead of waiting for another P to steal them. The price is a trip through the
// scheduler on every idle transition (and, on a multi-P process, usually a futex
// wake-up of another M), plus one extra non-blocking poll. A server that handles
// its traffic inline in the event-loop spawns no such goroutines, so for it the
// detour is pure overhead (measured at roughly a sixth of the server's CPU time
// per request when batches are small). Such pollers switch the yield off and go
// straight to the blocking wait; Options.NoPollYield does that for the engine.
type pollYield struct {
	noYield bool
}

// SetYield switches the yield-before-blocking behaviour described on pollYield on
// or off. It must be called before Polling starts; it is on by default.
func (y *pollYield) SetYield(enabled bool) { y.noYield = !enabled }
