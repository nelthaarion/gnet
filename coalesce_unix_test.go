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

package gnet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for write coalescing (see corkState): the Writes that an OnTraffic callback
// makes after its first one are buffered and sent in a single write(2) when the callback
// returns. Whatever the engine does with them, the byte stream the peer sees must be
// exactly the one that would have been produced by sending every Write immediately.

type coalesceServer struct {
	BuiltinEventEngine
	eng       Engine
	boot      chan struct{}
	onTraffic func(c Conn) Action
}

func (s *coalesceServer) OnBoot(eng Engine) Action {
	s.eng = eng
	close(s.boot)
	return None
}

func (s *coalesceServer) OnTraffic(c Conn) Action { return s.onTraffic(c) }

func startCoalesceServer(t *testing.T, onTraffic func(c Conn) Action, opts ...Option) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr = ln.Addr().String()
	_ = ln.Close()

	s := &coalesceServer{boot: make(chan struct{}), onTraffic: onTraffic}
	done := make(chan error, 1)
	go func() { done <- Run(s, "tcp4://"+addr, append([]Option{WithNumEventLoop(1)}, opts...)...) }()
	select {
	case <-s.boot:
	case err := <-done:
		t.Fatalf("server exited before boot: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not boot")
	}
	stop = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.eng.Stop(ctx)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	}
	return addr, stop
}

const coalesceReqSize = 8 // request: 4-byte big-endian index + 4 bytes of padding

func coalesceRequests(from, to int) []byte {
	var b bytes.Buffer
	for i := from; i < to; i++ {
		var r [coalesceReqSize]byte
		binary.BigEndian.PutUint32(r[:], uint32(i))
		b.Write(r[:])
	}
	return b.Bytes()
}

// coalesceResponse is the response to request i: its index followed by size-4 bytes of a pattern.
func coalesceResponse(i, size int) []byte {
	r := make([]byte, size)
	binary.BigEndian.PutUint32(r, uint32(i))
	for j := 4; j < size; j++ {
		r[j] = byte(i + j)
	}
	return r
}

// echoPattern answers every complete request in the inbound buffer with its own Write.
func echoPattern(respSize int) func(c Conn) Action {
	return func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, err := c.Next(coalesceReqSize)
			if err != nil {
				return Close
			}
			idx := int(binary.BigEndian.Uint32(buf))
			if _, err := c.Write(coalesceResponse(idx, respSize)); err != nil {
				return Close
			}
		}
		return None
	}
}

func dialCoalesce(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	conn, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	return conn.(*net.TCPConn)
}

func readAndVerify(t *testing.T, conn net.Conn, from, to, respSize int, deadline time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(deadline))
	got := make([]byte, respSize)
	for i := from; i < to; i++ {
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("response %d/%d: %v", i, to, err)
		}
		if want := coalesceResponse(i, respSize); !bytes.Equal(got, want) {
			t.Fatalf("response %d differs: got % x... want % x...", i, got[:min(16, len(got))], want[:min(16, len(want))])
		}
	}
}

func TestWriteCoalescingPreservesOrder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     []Option
		requests int
		respSize int
	}{
		{"single-request", nil, 1, 64},
		{"two-requests", nil, 2, 64},
		{"pipeline-16", nil, 16, 64},
		{"pipeline-500", nil, 500, 100},
		{"pipeline-16-ET", []Option{WithEdgeTriggeredIO(true)}, 16, 64},
		{"pipeline-16-noyield", []Option{WithNoPollYield(true)}, 16, 64},
		{"pipeline-16-no-coalescing", []Option{WithNoWriteCoalescing(true)}, 16, 64},
		{"pipeline-16-reuseport", []Option{WithReusePort(true)}, 16, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, stop := startCoalesceServer(t, echoPattern(tc.respSize), tc.opts...)
			defer stop()
			conn := dialCoalesce(t, addr)
			defer conn.Close()

			// Everything in one write, so that the server sees the whole batch in one read ...
			if _, err := conn.Write(coalesceRequests(0, tc.requests)); err != nil {
				t.Fatal(err)
			}
			readAndVerify(t, conn, 0, tc.requests, tc.respSize, 10*time.Second)

			// ... and then again, after the connection has been through a coalescing round.
			if _, err := conn.Write(coalesceRequests(tc.requests, tc.requests+3)); err != nil {
				t.Fatal(err)
			}
			readAndVerify(t, conn, tc.requests, tc.requests+3, tc.respSize, 10*time.Second)
		})
	}
}

// Responses much larger than the socket buffers: the flush at the end of the callback cannot
// send them in one go and has to hand the remainder to the poller (EPOLLOUT) while the peer
// reads slowly. Order and content must survive.
func TestWriteCoalescingBackpressure(t *testing.T) {
	const (
		requests = 6
		respSize = 300 * 1024
	)
	addr, stop := startCoalesceServer(t, echoPattern(respSize), WithSocketSendBuffer(32*1024))
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()
	if err := conn.SetReadBuffer(16 * 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(coalesceRequests(0, requests)); err != nil {
		t.Fatal(err)
	}
	// Read slowly, in small pieces.
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	total := requests * respSize
	stream := make([]byte, 0, total)
	chunk := make([]byte, 7000)
	for len(stream) < total {
		n, err := conn.Read(chunk)
		if err != nil {
			t.Fatalf("after %d/%d bytes: %v", len(stream), total, err)
		}
		stream = append(stream, chunk[:n]...)
		if len(stream)%(64*1024) < n {
			time.Sleep(time.Millisecond)
		}
	}
	for i := 0; i < requests; i++ {
		if !bytes.Equal(stream[i*respSize:(i+1)*respSize], coalesceResponse(i, respSize)) {
			t.Fatalf("response %d is corrupted or out of order", i)
		}
	}
}

// Write, Writev and Flush mixed inside one callback.
func TestWriteCoalescingWritevAndFlush(t *testing.T) {
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			a, b, d, e := coalesceResponse(idx, 10), coalesceResponse(idx, 20), coalesceResponse(idx, 30), coalesceResponse(idx, 40)
			_, _ = c.Write(a)               // first write: direct
			_, _ = c.Writev([][]byte{b, d}) // coalesced
			if idx%2 == 0 {
				_ = c.Flush() // explicit flush inside the callback
			}
			_, _ = c.Write(e) // after (maybe) a flush
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()

	const requests = 40
	if _, err := conn.Write(coalesceRequests(0, requests)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < requests; i++ {
		for _, size := range []int{10, 20, 30, 40} {
			got := make([]byte, size)
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatalf("request %d part %d: %v", i, size, err)
			}
			if !bytes.Equal(got, coalesceResponse(i, size)) {
				t.Fatalf("request %d part %d: wrong bytes", i, size)
			}
		}
	}
}

// A callback that writes several responses and then asks for the connection to be closed:
// everything it wrote has to arrive before the EOF.
func TestWriteCoalescingCloseAfterWrites(t *testing.T) {
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			_, _ = c.Write(coalesceResponse(idx, 50))
			if idx == 4 {
				return Close
			}
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()
	if _, err := conn.Write(coalesceRequests(0, 5)); err != nil {
		t.Fatal(err)
	}
	readAndVerify(t, conn, 0, 5, 50, 10*time.Second)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF after the last response, got n=%d err=%v", n, err)
	}
}

// AsyncWrite is executed after the callback returned, so it has to come after everything the
// callback wrote, whether that was sent directly or coalesced.
func TestWriteCoalescingWithAsyncWrite(t *testing.T) {
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			_, _ = c.Write(coalesceResponse(idx, 20))
			_, _ = c.Write(coalesceResponse(idx, 30))
			_ = c.AsyncWrite(coalesceResponse(idx, 40), nil)
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()
	const requests = 20
	if _, err := conn.Write(coalesceRequests(0, requests)); err != nil {
		t.Fatal(err)
	}
	// Per request the stream is: 20 bytes, 30 bytes (direct+coalesced), then the async 40 bytes
	// somewhere after the callback finished, i.e. after all `requests` synchronous pairs.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < requests; i++ {
		for _, size := range []int{20, 30} {
			got := make([]byte, size)
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatalf("sync part %d/%d: %v", i, size, err)
			}
			if !bytes.Equal(got, coalesceResponse(i, size)) {
				t.Fatalf("sync part %d/%d: wrong bytes", i, size)
			}
		}
	}
	for i := 0; i < requests; i++ {
		got := make([]byte, 40)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("async part %d: %v", i, err)
		}
		if !bytes.Equal(got, coalesceResponse(i, 40)) {
			t.Fatalf("async part %d: wrong bytes", i)
		}
	}
}

// The peer vanishes while a callback has queued far more than the kernel will take: the engine
// has to clean the connection up without panicking and keep serving others.
func TestWriteCoalescingPeerGoesAway(t *testing.T) {
	addr, stop := startCoalesceServer(t, echoPattern(200*1024), WithSocketSendBuffer(16*1024))
	defer stop()

	for i := 0; i < 5; i++ {
		conn := dialCoalesce(t, addr)
		_ = conn.SetLinger(0) // RST on close
		if _, err := conn.Write(coalesceRequests(0, 8)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		_ = conn.Close()
	}

	// A fresh client must still be served correctly.
	conn := dialCoalesce(t, addr)
	defer conn.Close()
	if _, err := conn.Write(coalesceRequests(0, 3)); err != nil {
		t.Fatal(err)
	}
	readAndVerify(t, conn, 0, 3, 200*1024, 15*time.Second)
}

// Many connections at once, each pipelining a different amount.
func TestWriteCoalescingManyConnections(t *testing.T) {
	addr, stop := startCoalesceServer(t, echoPattern(48))
	defer stop()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for c := 0; c < 32; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			conn, err := net.Dial("tcp4", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			n := 1 + c*7%40
			for round := 0; round < 20; round++ {
				if _, err := conn.Write(coalesceRequests(round*100, round*100+n)); err != nil {
					errs <- err
					return
				}
				_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				got := make([]byte, 48)
				for i := 0; i < n; i++ {
					if _, err := io.ReadFull(conn, got); err != nil {
						errs <- fmt.Errorf("conn %d round %d resp %d: %w", c, round, i, err)
						return
					}
					if !bytes.Equal(got, coalesceResponse(round*100+i, 48)) {
						errs <- fmt.Errorf("conn %d round %d resp %d: wrong bytes", c, round, i)
						return
					}
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// OutboundBuffered shows what the engine did with the Writes of one callback: the first one went
// straight to the kernel (nothing buffered), the later ones are buffered until the callback
// returns. In edge-triggered mode every write stays immediate.
func TestWriteCoalescingObservable(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts []Option
		want [3]int
	}{
		{"LT", nil, [3]int{0, 100, 200}},
		{"ET", []Option{WithEdgeTriggeredIO(true)}, [3]int{0, 0, 0}},
		{"LT-NoWriteCoalescing", []Option{WithNoWriteCoalescing(true)}, [3]int{0, 0, 0}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			seen := make(chan [3]int, 1)
			handler := func(c Conn) Action {
				if c.InboundBuffered() < 3*coalesceReqSize {
					return None // wait until the whole batch has arrived
				}
				var got [3]int
				for i := 0; i < 3; i++ {
					buf, _ := c.Next(coalesceReqSize)
					idx := int(binary.BigEndian.Uint32(buf))
					_, _ = c.Write(coalesceResponse(idx, 100))
					got[i] = c.OutboundBuffered()
				}
				select {
				case seen <- got:
				default:
				}
				return None
			}
			addr, stop := startCoalesceServer(t, handler, mode.opts...)
			defer stop()
			conn := dialCoalesce(t, addr)
			defer conn.Close()
			if _, err := conn.Write(coalesceRequests(0, 3)); err != nil {
				t.Fatal(err)
			}
			readAndVerify(t, conn, 0, 3, 100, 10*time.Second)
			select {
			case got := <-seen:
				if got != mode.want {
					t.Fatalf("OutboundBuffered after each Write = %v, want %v", got, mode.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("handler never saw the whole batch")
			}
		})
	}
}

// A connection whose previous callback wrote more than once is assumed to be pipelining: its next
// callback holds back even the first Write until it returns (corkHold), and a callback that then
// writes only once ends the guess. OutboundBuffered shows which mode a callback ran in; the byte
// stream the peer sees must be the same in every case.
func TestWriteCoalescingHoldsFirstWriteOfPipeliningConnection(t *testing.T) {
	var want atomic.Int32 // number of requests the next callback must have seen before it answers
	seen := make(chan [3]int, 8)
	handler := func(c Conn) Action {
		n := int(want.Load())
		if n == 0 || c.InboundBuffered() < n*coalesceReqSize {
			return None // wait until the whole batch has arrived
		}
		want.Store(0)
		var got [3]int
		for i := 0; i < n; i++ {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			_, _ = c.Write(coalesceResponse(idx, 100))
			if i < len(got) {
				got[i] = c.OutboundBuffered()
			}
		}
		seen <- got
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()

	next := 0
	for round, tc := range []struct {
		requests int
		want     [3]int // OutboundBuffered after each of the first three Writes
	}{
		{3, [3]int{0, 100, 200}},   // first callback of the connection: the first Write goes out directly
		{3, [3]int{100, 200, 300}}, // the previous callback wrote 3 times: everything is held back
		{1, [3]int{100, 0, 0}},     // still held back, but this callback writes only once ...
		{1, [3]int{0, 0, 0}},       // ... which ends the guess: the first Write goes out directly again
		{2, [3]int{0, 100, 0}},     // two Writes are enough to start it again ...
		{2, [3]int{100, 200, 0}},   // ... and the next callback holds back
	} {
		want.Store(int32(tc.requests))
		if _, err := conn.Write(coalesceRequests(next, next+tc.requests)); err != nil {
			t.Fatal(err)
		}
		readAndVerify(t, conn, next, next+tc.requests, 100, 10*time.Second)
		next += tc.requests
		select {
		case got := <-seen:
			if got != tc.want {
				t.Fatalf("round %d: OutboundBuffered after each Write = %v, want %v", round, got, tc.want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the handler never answered", round)
		}
	}
}

// The first Write of a pipelining connection is held back only while it is small: a large one is
// sent directly, as before, and everything has to arrive in order either way.
func TestWriteCoalescingHoldLargeFirstWrite(t *testing.T) {
	size := func(idx int) int {
		if idx%3 == 0 {
			return 3 * corkHoldMax // larger than corkHoldMax: never held back
		}
		return 40 + idx
	}
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			if _, err := c.Write(coalesceResponse(idx, size(idx))); err != nil {
				return Close
			}
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()

	verify := func(from, to int) {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		for i := from; i < to; i++ {
			got := make([]byte, size(i))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatalf("response %d: %v", i, err)
			}
			if !bytes.Equal(got, coalesceResponse(i, size(i))) {
				t.Fatalf("response %d is corrupted or out of order", i)
			}
		}
	}
	// 1, 2, 4, 5: small; 3 and 6 start the callbacks below with a large response.
	for _, r := range [][2]int{{1, 5}, {6, 12}, {12, 13}, {13, 20}, {21, 22}, {24, 30}} {
		if _, err := conn.Write(coalesceRequests(r[0], r[1])); err != nil {
			t.Fatal(err)
		}
		verify(r[0], r[1])
	}
}

// Writes that a callback held back must still be sent when it asks for the connection to be
// closed, and before the EOF.
func TestWriteCoalescingHoldThenClose(t *testing.T) {
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			_, _ = c.Write(coalesceResponse(idx, 50))
			if idx == 5 {
				return Close
			}
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()
	// The first callback writes three times, so the second one runs in hold mode.
	if _, err := conn.Write(coalesceRequests(0, 3)); err != nil {
		t.Fatal(err)
	}
	readAndVerify(t, conn, 0, 3, 50, 10*time.Second)
	if _, err := conn.Write(coalesceRequests(3, 6)); err != nil {
		t.Fatal(err)
	}
	readAndVerify(t, conn, 3, 6, 50, 10*time.Second)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF after the last response, got n=%d err=%v", n, err)
	}
}

// Writev and Flush inside a callback that runs in hold mode.
func TestWriteCoalescingHoldWritevAndFlush(t *testing.T) {
	handler := func(c Conn) Action {
		for c.InboundBuffered() >= coalesceReqSize {
			buf, _ := c.Next(coalesceReqSize)
			idx := int(binary.BigEndian.Uint32(buf))
			a, b, d := coalesceResponse(idx, 10), coalesceResponse(idx, 20), coalesceResponse(idx, 30)
			_, _ = c.Writev([][]byte{a, b})
			if idx%2 == 0 {
				_ = c.Flush()
			}
			_, _ = c.Write(d)
		}
		return None
	}
	addr, stop := startCoalesceServer(t, handler)
	defer stop()
	conn := dialCoalesce(t, addr)
	defer conn.Close()
	const requests = 30
	for _, r := range [][2]int{{0, 10}, {10, 20}, {20, 30}} { // every callback after the first runs in hold mode
		if _, err := conn.Write(coalesceRequests(r[0], r[1])); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		for i := r[0]; i < r[1]; i++ {
			for _, size := range []int{10, 20, 30} {
				got := make([]byte, size)
				if _, err := io.ReadFull(conn, got); err != nil {
					t.Fatalf("request %d part %d: %v", i, size, err)
				}
				if !bytes.Equal(got, coalesceResponse(i, size)) {
					t.Fatalf("request %d part %d: wrong bytes", i, size)
				}
			}
		}
	}
}
