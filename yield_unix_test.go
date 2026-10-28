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

//go:build linux

package gnet

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// The adaptive poll yield (see pollYield in pkg/netpoll) skips the scheduler yield of an idle event-loop
// once it has served a stretch of traffic inline. A server that then starts to hand its requests to other
// goroutines, which answer with AsyncWrite, must keep working at once and with sane latency, whatever the
// number of Ps and the yield policy are.
func TestAdaptiveYieldSwitchFromInlineToAsync(t *testing.T) {
	policies := []struct {
		name string
		opts []Option
	}{
		{"adaptive", nil},
		{"always", []Option{WithAlwaysPollYield(true)}},
		{"never", []Option{WithNoPollYield(true)}},
		{"adaptive-locked-thread", []Option{WithLockOSThread(true)}},
	}
	for _, procs := range []int{1, 2, 4} {
		for _, policy := range policies {
			procs, policy := procs, policy
			t.Run(fmt.Sprintf("GOMAXPROCS=%d/%s", procs, policy.name), func(t *testing.T) {
				testYieldSwitchFromInlineToAsync(t, procs, policy.opts)
			})
		}
	}
}

func testYieldSwitchFromInlineToAsync(t *testing.T, procs int, opts []Option) {
	prev := runtime.GOMAXPROCS(procs)
	defer runtime.GOMAXPROCS(prev)

	var async atomic.Bool
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			if !async.Load() {
				_, _ = c.Write(buf)
				continue
			}
			msg := append([]byte(nil), buf...)
			go func() { _ = c.AsyncWrite(msg, nil) }()
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler, opts...)
	defer stop()
	conn, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	roundTrip := func(i int) time.Duration {
		t.Helper()
		start := time.Now()
		_ = conn.SetDeadline(start.Add(10 * time.Second))
		if _, err := conn.Write(coalesceRequests(i, i+1)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, coalesceReqSize)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("round trip %d: %v", i, err)
		}
		if want := coalesceRequests(i, i+1); string(got) != string(want) {
			t.Fatalf("round trip %d: got % x, want % x", i, got, want)
		}
		return time.Since(start)
	}

	// Serve enough batches inline for the loop to leave its yield window.
	for i := 0; i < 3*1024; i++ {
		roundTrip(i)
	}
	// Now every answer comes from another goroutine.
	async.Store(true)
	var worst time.Duration
	for i := 0; i < 300; i++ {
		if d := roundTrip(i); d > worst {
			worst = d
		}
	}
	if worst > 2*time.Second {
		t.Fatalf("slowest async round trip took %v", worst)
	}
}
