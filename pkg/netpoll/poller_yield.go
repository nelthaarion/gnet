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

import "runtime"

// pollYield holds the poller's scheduler-yield policy; it is embedded in every
// Poller implementation so they all share one definition of it.
//
// A poller that finds nothing to do can call runtime.Gosched before it blocks in
// the kernel, so goroutines that the event callbacks just made runnable (worker
// pools, AsyncWrite producers, ...) get to run on this P straight away instead of
// waiting for another P to steal them. The price is a trip through the scheduler on
// every idle transition (and, on a multi-P process, usually a futex wake-up of
// another M that then finds nothing to run), plus one extra non-blocking poll. A
// server that handles its traffic inline in the event-loop spawns no such
// goroutines, so for it the detour is pure overhead (measured at roughly a sixth of
// the server's CPU time per request when batches are small).
//
// There are three policies:
//
//   - never yield: Options.NoPollYield, see SetYield;
//   - always yield: the behaviour of upstream gnet;
//   - adaptive, the engine's default for the event-loops that serve established
//     connections in a process with more Ps than event-loops (see SetAdaptiveYield):
//     yield only while goroutines can actually be waiting for this loop to give up
//     its P, which is when other goroutines have recently been handing work to the
//     loop through its task queues (AsyncWrite, Wake, Close and friends, accepted
//     connections), and skip the detour otherwise.
type pollYield struct {
	noYield  bool // never yield, see SetYield
	adaptive bool // yield only while there is cross-goroutine traffic, see SetAdaptiveYield
}

// yieldWindow is how many event batches an adaptive poller keeps yielding for
// after the last time it was woken through its task queues. A loop that has been
// quiet for that many batches handles its traffic inline, and the yield is skipped
// until the next wake-up through the queues. The count advances with batches and
// not with time, so a pause in traffic does not use it up.
const yieldWindow = 1024

// SetYield switches the yield-before-blocking behaviour described on pollYield on
// or off. It must be called before Polling starts; it is on by default.
func (y *pollYield) SetYield(enabled bool) { y.noYield = !enabled }

// SetAdaptiveYield makes an enabled yield adaptive: the poller yields only for
// yieldWindow batches after each wake-up through its task queues and goes straight
// to the blocking wait otherwise. It must be called before Polling starts, and has
// no effect while the yield is disabled by SetYield. Pollers other than the epoll
// ones ignore it and keep yielding unconditionally.
//
// Skipping the yield does not affect correctness, only when goroutines that the
// callbacks made runnable start: they stay in the run queue of this P and are picked
// up by an idle P (which the runtime wakes for them) or, when every P is busy, once
// the loop blocks in the kernel and the runtime hands its P over, which can take up
// to a scheduler monitor tick. That is why a process with a single P never skips the
// yield: there is no other P that could take the goroutines, and the yield itself is
// cheap there because no thread has to be woken.
func (y *pollYield) SetAdaptiveYield(adaptive bool) { y.adaptive = adaptive }

// adaptiveYield reports whether Polling should apply the adaptive policy; it is
// evaluated once, when Polling starts.
func (y *pollYield) adaptiveYield() bool {
	return y.adaptive && !y.noYield && runtime.GOMAXPROCS(0) > 1
}
