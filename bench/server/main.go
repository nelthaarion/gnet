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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nelthaarion/gnet/v2"
)

var (
	mode      = flag.String("mode", "echo", "echo|http")
	addr      = flag.String("addr", ":9000", "listen address")
	loops     = flag.Int("loops", 0, "number of event-loops, 0 = NumCPU")
	multicore = flag.Bool("multicore", true, "Multicore")
	reusePort = flag.Bool("reuseport", false, "SO_REUSEPORT (per-loop acceptors)")
	lb        = flag.String("lb", "rr", "rr|lc|hash")
	et        = flag.Bool("et", false, "edge-triggered I/O")
	readBuf   = flag.Int("rbuf", 0, "ReadBufferCap, 0 = gnet default")
	writeBuf  = flag.Int("wbuf", 0, "WriteBufferCap, 0 = gnet default")
	bodySize  = flag.Int("body", 12, "http response body size in bytes")
)

var (
	conns    atomic.Int64
	requests atomic.Int64
	errors   atomic.Int64
)

type server struct {
	gnet.BuiltinEventEngine
	keepAlive []byte // precomputed 200 response, read-only after boot
	closeResp []byte // same, but asks the client to close
}

func (s *server) OnBoot(eng gnet.Engine) gnet.Action {
	fmt.Fprintf(os.Stderr, "gnet-bench boot pid=%d mode=%s addr=%s loops=%d multicore=%v reuseport=%v lb=%s et=%v body=%d\n",
		os.Getpid(), *mode, *addr, *loops, *multicore, *reusePort, *lb, *et, *bodySize)
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
	if *mode == "http" {
		return s.serveHTTP(c)
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
	requests.Add(1)
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
		end := bytes.Index(buf, []byte("\r\n\r\n"))
		if end < 0 {
			if len(buf) > 1<<20 {
				return gnet.Close
			}
			return gnet.None
		}
		head := buf[:end+4]

		// Body length for the request, if any.
		need := 0
		for _, line := range strings.Split(string(head), "\r\n") {
			if k, v, ok := strings.Cut(line, ":"); ok && strings.EqualFold(k, "Content-Length") {
				need, _ = strconv.Atoi(strings.TrimSpace(v))
				break
			}
		}
		if len(buf) < end+4+need {
			return gnet.None // body still arriving
		}

		keepAlive := !strings.Contains(string(head), "Connection: close")
		var out []byte
		if keepAlive {
			out = s.keepAlive
		} else {
			out = s.closeResp
		}
		if _, err := c.Write(out); err != nil {
			return gnet.Close
		}
		if _, err := c.Discard(len(buf)); err != nil {
			return gnet.Close
		}
		requests.Add(1)
		if !keepAlive {
			return gnet.Close
		}
	}
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

	if strings.Contains(*addr, "://") == false {
		*addr = "tcp://" + *addr
	}

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			fmt.Fprintf(os.Stderr, "  conns=%d requests=%d errors=%d\n", conns.Load(), requests.Load(), errors.Load())
		}
	}()

	if err := gnet.Run(s, *addr, opts...); err != nil {
		fmt.Fprintln(os.Stderr, "gnet.Run:", err)
		os.Exit(1)
	}
}
