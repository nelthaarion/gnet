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

import "testing"

func TestEventListGrowsWhenFull(t *testing.T) {
	el := newEventList(InitPollEventsCap)
	el.adapt(el.size)
	if el.size != 2*InitPollEventsCap || len(el.events) != el.size {
		t.Fatalf("size=%d len=%d after a full poll", el.size, len(el.events))
	}
	for i := 0; i < 10; i++ { // growth is capped
		el.adapt(el.size)
	}
	if el.size != MaxPollEventsCap {
		t.Fatalf("size=%d, want the cap %d", el.size, MaxPollEventsCap)
	}
}

// A load that hovers around a power of two used to flip the array between two sizes on every
// poll, allocating each time.
func TestEventListDoesNotFlapAroundPowerOfTwo(t *testing.T) {
	el := newEventList(InitPollEventsCap) // 128
	resizes := 0
	last := el.size
	for i := 0; i < 10000; i++ {
		n := 63 + i%2 // 63, 64, 63, 64: the old policy shrank on 63 (< 64) and grew on 128
		if el.size == 64 {
			n = 64 // pretend it fills the smaller array
		}
		el.adapt(n)
		if el.size != last {
			resizes++
			last = el.size
		}
	}
	if resizes > 4 {
		t.Fatalf("event list resized %d times in 10000 polls", resizes)
	}
}

func TestEventListShrinksAfterSustainedLowLoad(t *testing.T) {
	el := newEventList(InitPollEventsCap)
	for i := 0; i < shrinkAfterPolls-1; i++ {
		el.adapt(1)
		if el.size != InitPollEventsCap {
			t.Fatalf("shrunk after only %d small polls", i+1)
		}
	}
	el.adapt(1)
	if el.size != InitPollEventsCap/2 {
		t.Fatalf("size=%d, want %d after %d small polls", el.size, InitPollEventsCap/2, shrinkAfterPolls)
	}
	for i := 0; i < 100*shrinkAfterPolls; i++ {
		el.adapt(0)
	}
	if el.size != MinPollEventsCap {
		t.Fatalf("size=%d, want the floor %d", el.size, MinPollEventsCap)
	}
}

func TestEventListBusyPollResetsShrinkStreak(t *testing.T) {
	el := newEventList(InitPollEventsCap)
	for round := 0; round < 50; round++ {
		for i := 0; i < shrinkAfterPolls-1; i++ {
			el.adapt(1)
		}
		el.adapt(InitPollEventsCap/2 + 1) // not small: the streak starts over
	}
	if el.size != InitPollEventsCap {
		t.Fatalf("size=%d, an interrupted low-load streak must not shrink the list", el.size)
	}
}
