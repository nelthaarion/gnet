// Command loadgen is a deliberately dependency-free load generator used to
// measure gnet's request throughput. It never imports gnet, so the client can
// never be the variable under study.
// Command loadgen is a deliberately dependency-free load generator used to
// measure gnet's request throughput. It never imports gnet, so the client can
// never be the variable under study.
//
// Two workloads:
//
//	echo - raw TCP echo: issue N piplelined requests, read N responses.
//	http - HTTP/1.1 keep-alive GET against a fixed-length response.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	addr     = flag.String("addr", "server:9000", "target host:port")
	conns    = flag.Int("c", 8, "number of connections (workers)")
	duration = flag.Duration("d", 10*time.Second, "test duration")
	msgSize  = flag.Int("s", 64, "request/response size in bytes")
	pipeline = flag.Int("p", 1, "pipeline depth: requests in flight per connection")
	mode     = flag.String("mode", "echo", "echo|http")
)

var (
	total    atomic.Int64
	failed   atomic.Int64
	startT   = time.Now()
	quitTime time.Time
)

func main() {
	flag.Parse()
	if *conns < 1 || *duration <= 0 || *msgSize < 1 || *pipeline < 1 {
		fmt.Fprintln(os.Stderr, "invalid flags")
		os.Exit(2)
	}
	quitTime = time.Now().Add(*duration)

	// Payload is fixed per connection so the client's own cost stays out of the
	// way; the server echoes it back byte for byte, which also verifies the
	// payload comes back intact.
	var wg sync.WaitGroup
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(id)
		}(i)
	}

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				el := time.Since(startT).Seconds()
				if el > 0 {
					fmt.Fprintf(os.Stderr, "\r  %10d reqs  %12.0f req/s   ", total.Load(), float64(total.Load())/el)
				}
			case <-done:
				return
			}
		}
	}()
	wg.Wait()
	close(done)

	elapsed := time.Since(startT).Seconds()
	fmt.Fprintf(os.Stderr, "\r%s\r", "")
	fmt.Printf("mode=%s conns=%d size=%d pipeline=%d rps=%d bytes_rx=%d failed=%d elapsed=%.2fs\n",
		*mode, *conns, *msgSize, *pipeline, int64(float64(total.Load())/elapsed), total.Load()*int64(*msgSize),
		failed.Load(), elapsed)
}

func worker(_ int) {
	for {
		if time.Now().After(quitTime) {
			return
		}
		nc, err := net.Dial("tcp", *addr)
		if err != nil {
			failed.Add(1)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if tc, ok := nc.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		run(nc)
		_ = nc.Close()
	}
}

// run drives one connection for the remainder of the test.
func run(nc net.Conn) {
	payload := bytes.Repeat([]byte{'a'}, *msgSize)
	req := append([]byte(nil), payload...)

	if *mode == "http" {
		runHTTP(nc)
		return
	}

	buf := make([]byte, *msgSize**pipeline)
	nc.SetReadDeadline(quitTime.Add(2 * time.Second))
	nc.SetWriteDeadline(quitTime.Add(2 * time.Second))
	for time.Now().Before(quitTime) {
		for i := 0; i < *pipeline; i++ {
			if _, err := nc.Write(req); err != nil {
				failed.Add(1)
				return
			}
		}
		got := 0
		for got < len(buf) {
			n, err := nc.Read(buf[got:])
			if err != nil {
				failed.Add(1)
				return
			}
			got += n
		}
		total.Add(int64(*pipeline))
	}
}

// runHTTP issues keep-alive GET requests and parses just enough of the reply
// to know when the next request can be written.
func runHTTP(nc net.Conn) {
	br := bufio.NewReaderSize(nc, 64*1024)
	head := []byte("GET /bench HTTP/1.1\r\nHost: " + *addr + "\r\nConnection: keep-alive\r\n\r\n")
	respHead := make([]byte, 0, 128)
	nc.SetReadDeadline(quitTime.Add(2 * time.Second))
	nc.SetWriteDeadline(quitTime.Add(2 * time.Second))
	for time.Now().Before(quitTime) {
		if _, err := nc.Write(head); err != nil {
			failed.Add(1)
			return
		}
		// Read the status line and headers.
		respHead = respHead[:0]
		for {
			line, err := br.ReadSlice('\n')
			if err != nil {
				failed.Add(1)
				return
			}
			respHead = append(respHead, line...)
			if bytes.Equal(bytes.TrimRight(line, "\r\n"), []byte("")) {
				break
			}
		}
		// Content-Length is mandatory in our server responses.
		var cl int64 = -1
		for _, hl := range bytes.Split(respHead, []byte("\r\n")) {
			if bytes.HasPrefix(bytes.ToLower(hl), []byte("content-length:")) {
				fmt.Sscanf(string(bytes.TrimSpace(hl[15:])), "%d", &cl)
				break
			}
		}
		if cl <= 0 {
			failed.Add(1)
			return
		}
		if _, err := br.Discard(int(cl)); err != nil {
			failed.Add(1)
			return
		}
		total.Add(1)
	}
}
