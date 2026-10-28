// Command bench-server is the gnet server used by the benchmark suite.
//
// It exposes two workloads that are driven by the same engine configuration
// knobs, so the only thing that changes between runs is the gnet build:
//
//	echo  - raw TCP echo: request -> response, 1:1, no protocol overhead.
//	http  - a minimal HTTP/1.1 keep-alive responder with a fixed body,
//	        which is what an external tool (bombardier) can hammer.
//
// Every engine knob is a flag so the same binary can be re-run with different
// NumEventLoop / Multicore / LB / edge-triggered settings.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nelthaarion/gnet/v2"
)

var (
	mode       = flag.String("mode", "echo", "echo|http|async  (async: echo from a goroutine per request via AsyncWrite, i.e. work handed off the event-loop)")
	addr       = flag.String("addr", ":9000", "listen address")
	loops      = flag.Int("loops", 0, "number of event-loops, 0 = NumCPU")
	multicore  = flag.Bool("multicore", true, "Multicore")
	reusePort  = flag.Bool("reuseport", false, "SO_REUSEPORT (per-loop acceptors)")
	lb         = flag.String("lb", "rr", "rr|lc|hash")
	et         = flag.Bool("et", false, "edge-triggered I/O")
	readBuf    = flag.Int("rbuf", 0, "ReadBufferCap, 0 = gnet default")
	writeBuf   = flag.Int("wbuf", 0, "WriteBufferCap, 0 = gnet default")
	bodySize   = flag.Int("body", 12, "http response body size in bytes")
	noYield    = flag.Bool("noyield", false, "NoPollYield: do not Gosched before blocking in the poller (inline-processing servers)")
	forceYield = flag.Bool("alwaysyield", false, "AlwaysPollYield: Gosched before every blocking wait, as upstream gnet does (default: adaptive)")
	lockOS     = flag.Bool("lockthread", false, "LockOSThread for every event-loop")
	noCoalesce = flag.Bool("nocoalesce", false, "NoWriteCoalescing: every Write is its own write(2), as in upstream gnet")
	cpuProf    = flag.String("cpuprofile", "", "write a CPU profile to this file; flushed on SIGINT/SIGTERM")
)

// httpMode and asyncMode are *mode == "http" / "async", resolved once so that OnTraffic does not
// compare strings per request.
var httpMode, asyncMode bool

var (
	conns  atomic.Int64
	errors atomic.Int64
)

// requests is a striped counter. A single atomic.Int64 incremented by every
// event-loop is one cache line that all cores fight over, which caps multi-core
// scaling of the harness itself and would hide what the engine does; the stripe is
// picked by the connection's descriptor (stable per connection, spread across loops).
var requests [64]struct {
	n atomic.Int64
	_ [56]byte // pad to a cache line
}

func countRequest(c gnet.Conn) { requests[c.Fd()&63].n.Add(1) }

func totalRequests() (t int64) {
	for i := range requests {
		t += requests[i].n.Load()
	}
	return
}

type server struct {
	gnet.BuiltinEventEngine
	keepAlive []byte // precomputed 200 response, read-only after boot
	closeResp []byte // same, but asks the client to close
}

func (s *server) OnBoot(eng gnet.Engine) gnet.Action {
	fmt.Fprintf(os.Stderr, "gnet-bench boot pid=%d mode=%s addr=%s loops=%d multicore=%v reuseport=%v lb=%s et=%v body=%d noyield=%v lockthread=%v\n",
		os.Getpid(), *mode, *addr, *loops, *multicore, *reusePort, *lb, *et, *bodySize, *noYield, *lockOS)
	return gnet.None
}

func (s *server) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	conns.Add(1)
	return nil, gnet.None
}

func (s *server) OnClose(c gnet.Conn, err error) gnet.Action {
	conns.Add(-1)
	return gnet.None
}

func (s *server) OnTick() (time.Duration, gnet.Action) { return 5 * time.Second, gnet.None }

func (s *server) OnTraffic(c gnet.Conn) gnet.Action {
	if httpMode {
		return s.serveHTTP(c)
	}
	if asyncMode {
		return s.serveAsync(c)
	}
	// Fast path: echo whatever arrived, in as few calls as possible.
	// Peek(-1) hands back the whole readable run without copying; Write sends
	// it; Discard consumes it.
	buf, err := c.Peek(-1)
	if err != nil {
		return gnet.Close
	}
	if len(buf) == 0 {
		return gnet.None
	}
	n, err := c.Write(buf)
	if err != nil {
		errors.Add(1)
		return gnet.Close
	}
	if _, err := c.Discard(n); err != nil {
		errors.Add(1)
		return gnet.Close
	}
	countRequest(c)
	return gnet.None
}

func (s *server) serveHTTP(c gnet.Conn) gnet.Action {
	for {
		buf, err := c.Peek(-1)
		if err != nil {
			return gnet.Close
		}
		if len(buf) == 0 {
			return gnet.None
		}
		// Headers must be complete before we can answer.
		end := bytes.Index(buf, crlfcrlf)
		if end < 0 {
			if len(buf) > 1<<20 {
				return gnet.Close
			}
			return gnet.None
		}

		// Body length and Connection header of this request. The parse does not
		// allocate: it used to build a string and a []string per request, which made
		// the harness's own garbage a visible part of the "http" numbers.
		need, wantClose := parseHead(buf[:end+4])
		consumed := end + 4 + need
		if len(buf) < consumed {
			return gnet.None // body still arriving
		}

		out := s.keepAlive
		if wantClose {
			out = s.closeResp
		}
		if _, err := c.Write(out); err != nil {
			return gnet.Close
		}
		// Consume exactly this request. The old code discarded len(buf), i.e. every
		// byte that had arrived, which silently dropped pipelined requests that
		// followed the first one and left the client waiting for answers that never
		// came - the "http" workload could not be pipelined at all.
		if _, err := c.Discard(consumed); err != nil {
			return gnet.Close
		}
		countRequest(c)
		if wantClose {
			return gnet.Close
		}
	}
}

// serveAsync models a server whose handlers run outside the event-loop: the request is copied,
// a goroutine is started for it, and the answer goes back through AsyncWrite. It exists to
// measure how the engine behaves when goroutines are made runnable by the callbacks, which is
// the case that the poller's idle-time Gosched (and Options.NoPollYield) is about.
func (s *server) serveAsync(c gnet.Conn) gnet.Action {
	buf, err := c.Next(-1)
	if err != nil {
		return gnet.Close
	}
	if len(buf) == 0 {
		return gnet.None
	}
	data := append([]byte(nil), buf...)
	countRequest(c)
	go func() { _ = c.AsyncWrite(data, nil) }()
	return gnet.None
}

var crlfcrlf = []byte("\r\n\r\n")

// parseHead extracts the two things the responder cares about from a complete
// request head (request line + headers + the blank line): the Content-Length (0 when
// absent) and whether the client asked for "Connection: close". Header names and the
// token "close" are matched case-insensitively, as HTTP requires.
func parseHead(head []byte) (contentLength int, wantClose bool) {
	for len(head) > 0 {
		var line []byte
		if i := bytes.IndexByte(head, '\n'); i >= 0 {
			line, head = head[:i], head[i+1:]
		} else {
			line, head = head, nil
		}
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		switch {
		case hasPrefixFold(line, "content-length:"):
			contentLength = 0
			for _, ch := range line[len("content-length:"):] {
				if ch >= '0' && ch <= '9' {
					contentLength = contentLength*10 + int(ch-'0')
				} else if ch != ' ' && ch != '\t' {
					break
				}
			}
		case hasPrefixFold(line, "connection:"):
			if containsFold(line[len("connection:"):], "close") {
				wantClose = true
			}
		}
	}
	return
}

func lower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 'a' - 'A'
	}
	return b
}

// hasPrefixFold reports whether s starts with the lower-case ASCII prefix p, ignoring case.
func hasPrefixFold(s []byte, p string) bool {
	if len(s) < len(p) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if lower(s[i]) != p[i] {
			return false
		}
	}
	return true
}

// containsFold reports whether s contains the lower-case ASCII word w, ignoring case.
func containsFold(s []byte, w string) bool {
	for i := 0; i+len(w) <= len(s); i++ {
		if hasPrefixFold(s[i:], w) {
			return true
		}
	}
	return false
}

func buildResponse(conn string, n int) []byte {
	b := make([]byte, 0, 160+n)
	b = append(b, "HTTP/1.1 200 OK\r\nServer: gnet-bench\r\nContent-Type: text/plain\r\nContent-Length: "...)
	b = strconv.AppendInt(b, int64(n), 10)
	b = append(b, "\r\nConnection: "...)
	b = append(b, conn...)
	b = append(b, "\r\n\r\n"...)
	b = append(b, bytes.Repeat([]byte{'x'}, n)...)
	return b
}

func main() {
	flag.Parse()

	httpMode = *mode == "http"
	asyncMode = *mode == "async"

	if *cpuProf != "" {
		f, err := os.Create(*cpuProf)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cpuprofile:", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "cpuprofile:", err)
			os.Exit(1)
		}
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sig
			pprof.StopCPUProfile()
			_ = f.Close()
			os.Exit(0)
		}()
	}

	s := new(server)
	s.keepAlive = buildResponse("keep-alive", *bodySize)
	s.closeResp = buildResponse("close", *bodySize)

	opts := []gnet.Option{
		gnet.WithMulticore(*multicore),
		gnet.WithReusePort(*reusePort),
		gnet.WithReadBufferCap(*readBuf),
		gnet.WithWriteBufferCap(*writeBuf),
	}
	switch *lb {
	case "lc":
		opts = append(opts, gnet.WithLoadBalancing(gnet.LeastConnections))
	case "hash":
		opts = append(opts, gnet.WithLoadBalancing(gnet.SourceAddrHash))
	default:
		opts = append(opts, gnet.WithLoadBalancing(gnet.RoundRobin))
	}
	if *loops > 0 {
		opts = append(opts, gnet.WithNumEventLoop(*loops))
	}
	if *et {
		opts = append(opts, gnet.WithEdgeTriggeredIO(true))
	}
	if *noYield {
		opts = append(opts, gnet.WithNoPollYield(true))
	}
	if *forceYield {
		opts = append(opts, gnet.WithAlwaysPollYield(true))
	}
	if *noCoalesce {
		opts = append(opts, gnet.WithNoWriteCoalescing(true))
	}
	if *lockOS {
		opts = append(opts, gnet.WithLockOSThread(true))
	}

	if strings.Contains(*addr, "://") == false {
		*addr = "tcp://" + *addr
	}

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			fmt.Fprintf(os.Stderr, "  conns=%d requests=%d errors=%d\n", conns.Load(), totalRequests(), errors.Load())
		}
	}()

	if err := gnet.Run(s, *addr, opts...); err != nil {
		fmt.Fprintln(os.Stderr, "gnet.Run:", err)
		os.Exit(1)
	}
}
