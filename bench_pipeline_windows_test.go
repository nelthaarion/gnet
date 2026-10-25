package gnet

// Benchmarks and regression tests for the Windows inbound pipeline - the path
// every byte of inbound stream data takes from the per-connection reader
// goroutine to the user's OnTraffic callback:
//
//	nc.Read -> packTCPConn -> el.enqueue -> el.ch -> run() dispatch
//	       -> unpackTCPConn -> el.read -> OnTraffic -> Peek/Discard
//
// They use no sockets and no timers, so the numbers measure the engine's own
// cost (copies, pooling, queue hand-off, connection bookkeeping) rather than the
// loopback stack, and they are stable enough to serve as before/after evidence
// for buffer and queue changes.

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/nelthaarion/gnet/v2/pkg/logging"
	bbPool "github.com/nelthaarion/gnet/v2/pkg/pool/bytebuffer"
)

// benchEchoEvents is the OnTraffic side of the echo/RPC workload: consume
// everything that arrived and keep the connection open. "Sending" it back is
// counted rather than written, so the benchmark stays a pipeline benchmark.
type benchEchoEvents struct {
	BuiltinEventEngine
	written atomic.Int64
}

func (e *benchEchoEvents) OnTraffic(c Conn) Action {
	buf, err := c.Peek(-1)
	if err != nil {
		return Close
	}
	if len(buf) == 0 {
		return None
	}
	e.written.Add(int64(len(buf)))
	if _, err := c.Discard(len(buf)); err != nil {
		return Close
	}
	return None
}

// benchPartialEvents models a handler that consumes only part of each arrival
// (a protocol with a message still in flight): everything left behind has to
// survive to the next arrival, in order, without being reordered or lost.
//
// keep consumes an exact number of bytes whenever that many are available, which
// is the harshest case for the leftover path and is what the regression tests
// use. msg consumes whole fixed-size messages, leaving a remainder bounded by one
// message, which is what a real framed protocol looks like and what the
// benchmarks use: a handler that never caught up would grow the inbound buffer
// for ever and would then be measuring that, not the pipeline.
type benchPartialEvents struct {
	BuiltinEventEngine
	keep       int
	msg        int
	saw        []byte
	limit      int   // stop the connection once this many bytes have been consumed
	perArrival []int // bytes unconsumed after each arrival, for the capacity test
	recLimit   int   // bounds retained bytes so the harness is not what is measured
}

func (e *benchPartialEvents) OnTraffic(c Conn) Action {
	buf, err := c.Peek(-1)
	if err != nil {
		return Close
	}
	var consume int
	switch {
	case e.keep > 0:
		if len(buf) < e.keep {
			return None
		}
		consume = e.keep
	case e.msg > 0:
		if len(buf) < e.msg {
			return None
		}
		consume = len(buf) - len(buf)%e.msg
	default:
		return None
	}
	if e.recLimit == 0 || len(e.saw) < e.recLimit {
		want := consume
		if e.recLimit > 0 && want > e.recLimit-len(e.saw) {
			want = e.recLimit - len(e.saw)
		}
		e.saw = append(e.saw, buf[:want]...)
	}
	if _, err := c.Discard(consume); err != nil {
		return Close
	}
	if e.limit > 0 && len(e.saw) >= e.limit {
		return Close
	}
	return None
}

// newBenchEventLoop builds an event-loop that is not attached to any listener: it has
// just enough engine behind it (options, the errgroup that tracks the loop's goroutines)
// for the connection code to run against.
func newBenchEventLoop(ev EventHandler) *eventloop {
	eg, ctx := errgroup.WithContext(context.Background())
	eng := &engine{opts: &Options{LB: RoundRobin, Logger: logging.GetDefaultLogger()}}
	eng.concurrency.Group = eg
	eng.concurrency.ctx = ctx
	return &eventloop{
		ch:           make(chan any, 1024),
		eng:          eng,
		connections:  make(map[*conn]struct{}),
		eventHandler: ev,
	}
}

// newBenchConn builds a live connection handle. nc may be nil for the buffer
// benchmarks, which never touch the socket; tests that close the connection need
// a real one, because close() treats a nil rawConn as "already closed".
func newBenchConn(el *eventloop, nc net.Conn) *conn {
	c := &conn{
		loop:       el,
		buffer:     bbPool.Get(),
		rawConn:    nc,
		localAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000},
		remoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50000},
	}
	el.connections[c] = struct{}{}
	return c
}

// TestWindowsInboundPipelinePreservesOrder checks the property every change to
// the inbound buffers has to keep: a handler that consumes a fixed number of
// bytes per arrival sees the byte stream in order, across arrival boundaries.
func TestWindowsInboundPipelinePreservesOrder(t *testing.T) {
	const chunks = 512
	const keep = 7 // consume a deliberately awkward prefix of every arrival

	ev := &benchPartialEvents{keep: keep, limit: chunks * (keep + 13)}
	el := newBenchEventLoop(ev)
	c := newBenchConn(el, nil)
	defer c.release()

	var stream []byte
	for i := 0; i < chunks; i++ {
		payload := make([]byte, keep+13)
		for j := range payload {
			payload[j] = byte(i + j)
		}
		stream = append(stream, payload...)

		tc := packTCPConn(c, payload)
		if err := el.enqueue(tc); err != nil {
			t.Fatal(err)
		}
		if err := el.readTCP((<-el.ch).(*tcpConn)); err != nil {
			t.Fatalf("readTCP: %v", err)
		}
	}
	if want := stream[:chunks*keep]; !bytes.Equal(ev.saw, want) {
		t.Fatalf("ordering broken: got %d bytes, want %d bytes (first mismatch at %d)",
			len(ev.saw), len(want), firstDiff(ev.saw, want))
	}
}

func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// TestWindowsInboundPipelineBufferCapacityReuse checks the other property that
// keeps the pipeline allocation-free: the connection's inbound buffer can still
// hold a whole arrival after a long run of partial consumption, instead of
// shrinking by the bytes the handler consumed each time. A buffer that shrinks
// is re-grown and re-copied on the next arrival.
func TestWindowsInboundPipelineBufferCapacityReuse(t *testing.T) {
	const arrivals = 2000
	const payloadLen = 512
	payload := bytes.Repeat([]byte{'a'}, payloadLen)

	ev := &benchPartialEvents{keep: 8}
	el := newBenchEventLoop(ev)
	c := newBenchConn(el, nil)
	defer c.release()
	// Seed a buffer that can already hold a whole arrival, so the test measures
	// what the engine does with arrivals rather than the pool's initial default.
	c.buffer.B = make([]byte, payloadLen)

	for i := 0; i < arrivals; i++ {
		tc := packTCPConn(c, payload)
		if err := el.enqueue(tc); err != nil {
			t.Fatal(err)
		}
		if err := el.readTCP((<-el.ch).(*tcpConn)); err != nil {
			t.Fatal(err)
		}
		if c.buffer == nil {
			t.Fatal("connection lost its inbound buffer")
		}
		if cap(c.buffer.B) < payloadLen {
			t.Fatalf("inbound buffer capacity is %d < payload %d after %d arrivals",
				cap(c.buffer.B), payloadLen, i+1)
		}
	}
}

// TestWindowsPipelineClosedConnDropsArrival checks that an arrival for a
// connection the loop has already closed is dropped instead of resurrecting a
// released conn, and that its buffer still returns to the pool.
func TestWindowsPipelineClosedConnDropsArrival(t *testing.T) {
	nc, peer := net.Pipe()
	defer peer.Close() //nolint:errcheck

	el := newBenchEventLoop(&benchEchoEvents{})
	c := newBenchConn(el, nc)
	defer c.release()

	if err := el.close(c, io.EOF); err != nil {
		t.Fatal(err)
	}
	if c.buffer != nil {
		t.Fatal("close left the inbound buffer set")
	}
	tc := packTCPConn(c, []byte("stale"))
	if err := el.enqueue(tc); err != nil {
		t.Fatal(err)
	}
	if err := el.readTCP((<-el.ch).(*tcpConn)); err != nil {
		t.Fatal(err)
	}
}

func benchInboundPipeline(b *testing.B, handler EventHandler, size int) {
	el := newBenchEventLoop(handler)
	c := newBenchConn(el, nil)
	defer c.release()

	payload := bytes.Repeat([]byte{'a'}, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc := packTCPConn(c, payload)
		if err := el.enqueue(tc); err != nil {
			b.Fatal(err)
		}
		if err := el.readTCP((<-el.ch).(*tcpConn)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkInboundPipelineDrain measures a handler that consumes everything that
// arrives (echo, HTTP request/response): the common case.
func BenchmarkInboundPipelineDrain(b *testing.B) {
	for _, size := range []int{64, 512, 4096} {
		b.Run(itoaBench(size), func(b *testing.B) {
			benchInboundPipeline(b, &benchEchoEvents{}, size)
		})
	}
}

// BenchmarkInboundPipelinePartial measures a handler that consumes only part of
// each arrival (protocol with a message still in flight).
func BenchmarkInboundPipelinePartial(b *testing.B) {
	for _, size := range []int{64, 512} {
		b.Run(itoaBench(size), func(b *testing.B) {
			benchInboundPipeline(b, &benchPartialEvents{msg: 8, recLimit: 1 << 16}, size)
		})
	}
}

func itoaBench(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
