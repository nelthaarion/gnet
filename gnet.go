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

// Package gnet implements a high-performance, lightweight, non-blocking,
// event-driven networking framework written in pure Go.
//
// Visit https://gnet.host/ for more details about gnet.
package gnet

import (
	"context"
	"io"
	"net"
	"net/url"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/nelthaarion/gnet/v2/internal/gfd"
	"github.com/nelthaarion/gnet/v2/pkg/buffer/ring"
	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
	"github.com/nelthaarion/gnet/v2/pkg/math"
)

// Action is an action that occurs after the completion of an event.
type Action int

const (
	// None indicates that no action should occur following an event.
	None Action = iota

	// Close closes the connection.
	Close

	// Shutdown shutdowns the engine.
	Shutdown
)

// Engine represents an engine context which provides some functions.
type Engine struct {
	// eng is the internal engine struct.
	eng *engine
}

// Validate checks whether the engine is available.
func (e Engine) Validate() error {
	if e.eng == nil || len(e.eng.listeners) == 0 {
		return errorx.ErrEmptyEngine
	}
	if e.eng.isShutdown() {
		return errorx.ErrEngineInShutdown
	}
	return nil
}

// CountConnections counts the number of currently active connections and returns it.
func (e Engine) CountConnections() (count int) {
	if e.Validate() != nil {
		return -1
	}

	e.eng.eventLoops.iterate(func(_ int, el *eventloop) bool {
		count += int(el.countConn())
		return true
	})
	return
}

// Register registers the new connection to the event-loop that is chosen
// based off of the algorithm set by WithLoadBalancing.
// You should call either of the NewNetConnContext or NewNetAddrContext
// and pass the returned context to this method. net.Conn will precede
// net.Addr if both are present in the context.
//
// Note that you need to switch to another load-balancing algorithm over
// the default RoundRobin when starting the engine, to avoid data race
// issue if you plan on calling this method from somewhere later on.
func (e Engine) Register(ctx context.Context) (<-chan RegisteredResult, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}

	if e.eng.eventLoops.len() == 0 {
		return nil, errorx.ErrEmptyEngine
	}

	c, ok := FromNetConnContext(ctx)
	if ok {
		return e.eng.eventLoops.next(c.RemoteAddr()).Enroll(ctx, c)
	}

	addr, ok := FromNetAddrContext(ctx)
	if ok {
		return e.eng.eventLoops.next(addr).Register(ctx, addr)
	}

	return nil, errorx.ErrInvalidNetworkAddress
}

// Dup returns a copy of the underlying file descriptor of listener.
// It is the caller's responsibility to close dupFD when finished.
// Closing listener does not affect dupFD, and closing dupFD does not affect listener.
//
// Note that this method is only available when the engine has only one listener.
func (e Engine) Dup() (fd int, err error) {
	if err := e.Validate(); err != nil {
		return -1, err
	}

	if len(e.eng.listeners) > 1 {
		return -1, errorx.ErrUnsupportedOp
	}

	for _, ln := range e.eng.listeners {
		fd, err = ln.dup()
	}

	return
}

// DupListener is like Dup, but it duplicates the listener with the given network and address.
// This is useful when there are multiple listeners.
func (e Engine) DupListener(network, addr string) (int, error) {
	if err := e.Validate(); err != nil {
		return -1, err
	}

	for _, ln := range e.eng.listeners {
		if ln.network == network && ln.address == addr {
			return ln.dup()
		}
	}

	return -1, errorx.ErrInvalidNetworkAddress
}

// Stop gracefully shuts down this Engine without interrupting any active event-loops,
// it waits indefinitely for connections and event-loops to be closed and then shuts down.
func (e Engine) Stop(ctx context.Context) error {
	if err := e.Validate(); err != nil {
		return err
	}

	e.eng.shutdown(nil)

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()
	for {
		if e.eng.isShutdown() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Reader is an interface that consists of a number of methods for reading that Conn must implement.
//
// Note that the methods in this interface are not concurrency-safe for concurrent use,
// you must invoke them within any method in EventHandler.
type Reader interface {
	io.Reader
	io.WriterTo

	// Next returns the next n bytes and advances the inbound buffer.
	// buf must not be used in a new goroutine. Otherwise, use Read instead.
	//
	// If the number of the available bytes is less than requested,
	// a pair of (0, io.ErrShortBuffer) is returned.
	Next(n int) (buf []byte, err error)

	// Peek returns the next n bytes without advancing the inbound buffer,
	// the returned bytes remain valid until a Discard is called.
	// buf must neither be used in a new goroutine nor anywhere after the call
	// to Discard, make a copy of buf manually or use Read otherwise.
	//
	// If the number of the available bytes is less than requested,
	// a pair of (0, io.ErrShortBuffer) is returned.
	Peek(n int) (buf []byte, err error)

	// Discard advances the inbound buffer with next n bytes, returning the number of bytes discarded.
	Discard(n int) (discarded int, err error)

	// InboundBuffered returns the number of bytes that can be read from the current buffer.
	InboundBuffered() int
}

// Writer is an interface that consists of a number of methods for writing that Conn must implement.
type Writer interface {
	io.Writer     // not concurrency-safe
	io.ReaderFrom // not concurrency-safe

	// SendTo transmits a message to the given address, it's not concurrency-safe.
	SendTo(buf []byte, addr net.Addr) (n int, err error)

	// Writev writes multiple byte slices to remote synchronously, it's not concurrency-safe.
	Writev(bs [][]byte) (n int, err error)

	// Flush writes any buffered data to the underlying connection, it's not concurrency-safe.
	Flush() error

	// OutboundBuffered returns the number of bytes that can be read from the current buffer.
	OutboundBuffered() int

	// AsyncWrite writes bytes to remote asynchronously, it's concurrency-safe.
	AsyncWrite(buf []byte, callback AsyncCallback) (err error)

	// AsyncWritev writes multiple byte slices to remote asynchronously.
	AsyncWritev(bs [][]byte, callback AsyncCallback) (err error)
}

// AsyncCallback is a callback that will be invoked after the asynchronous function finishes.
type AsyncCallback func(c Conn, err error) error

// Socket is a set of functions which manipulate the underlying file descriptor of a connection.
type Socket interface {
	Fd() int
	Dup() (int, error)
	SetReadBuffer(size int) error
	SetWriteBuffer(size int) error
	SetLinger(secs int) error
	SetKeepAlivePeriod(d time.Duration) error
	SetKeepAlive(enabled bool, idle, intvl time.Duration, cnt int) error
	SetNoDelay(noDelay bool) error
}

// Runnable defines the common protocol of an execution on an event-loop.
type Runnable interface {
	Run(ctx context.Context) error
}

// RunnableFunc is an adapter to allow the use of ordinary function as a Runnable.
type RunnableFunc func(ctx context.Context) error

// Run executes the RunnableFunc itself.
func (fn RunnableFunc) Run(ctx context.Context) error {
	return fn(ctx)
}

// RegisteredResult is the result of a Register call.
type RegisteredResult struct {
	Conn Conn
	Err  error
}

// EventLoop provides a set of methods for manipulating the event-loop.
type EventLoop interface {
	Register(ctx context.Context, addr net.Addr) (<-chan RegisteredResult, error)
	Enroll(ctx context.Context, c net.Conn) (<-chan RegisteredResult, error)
	Execute(ctx context.Context, runnable Runnable) error
	Schedule(ctx context.Context, runnable Runnable, delay time.Duration) error
	Close(Conn) error
}

// Conn is an interface of underlying connection.
type Conn interface {
	Reader
	Writer
	Socket

	Context() (ctx any)
	SafeContext() (ctx any)
	EventLoop() EventLoop
	SetContext(ctx any)
	SetSafeContext(ctx any)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Wake(callback AsyncCallback) error
	CloseWithCallback(callback AsyncCallback) error
	Close() error
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

type (
	// EventHandler represents the engine events' callbacks for the Run call.
	EventHandler interface {
		OnBoot(eng Engine) (action Action)
		OnShutdown(eng Engine)
		OnOpen(c Conn) (out []byte, action Action)
		OnClose(c Conn, err error) (action Action)
		OnTraffic(c Conn) (action Action)
		OnTick() (delay time.Duration, action Action)
	}

	// BuiltinEventEngine is a built-in implementation of EventHandler which feeds
	// each method with an empty implementation.
	BuiltinEventEngine struct{}
)

func (*BuiltinEventEngine) OnBoot(_ Engine) (action Action)          { return }
func (*BuiltinEventEngine) OnShutdown(_ Engine)                      {}
func (*BuiltinEventEngine) OnOpen(_ Conn) (out []byte, action Action) { return }
func (*BuiltinEventEngine) OnClose(_ Conn, _ error) (action Action)  { return }
func (*BuiltinEventEngine) OnTraffic(_ Conn) (action Action)         { return }
func (*BuiltinEventEngine) OnTick() (delay time.Duration, action Action) { return }

// MaxStreamBufferCap is the default buffer size for each stream-oriented connection(TCP/Unix).
var MaxStreamBufferCap = 64 * 1024 // 64KB

func createListeners(addrs []string, opts ...Option) ([]*listener, *Options, error) {
	options := loadOptions(opts...)

	logger, logFlusher := logging.GetDefaultLogger(), logging.GetDefaultFlusher()
	if options.Logger == nil {
		if options.LogPath != "" {
			logger, logFlusher, _ = logging.CreateLoggerAsLocalFile(options.LogPath, options.LogLevel)
		}
		options.Logger = logger
	} else {
		logger = options.Logger
		logFlusher = nil
	}
	logging.SetDefaultLoggerAndFlusher(logger, logFlusher)

	logging.Debugf("default logging level is %s", logging.LogLevel())

	if options.LockOSThread && options.NumEventLoop > 10000 {
		logging.Errorf("too many event-loops under LockOSThread mode, should be less than 10,000 "+
			"while you are trying to set up %d\n", options.NumEventLoop)
		return nil, nil, errorx.ErrTooManyEventLoopThreads
	}

	if options.EdgeTriggeredIOChunk > 0 {
		options.EdgeTriggeredIO = true
		options.EdgeTriggeredIOChunk = math.CeilToPowerOfTwo(options.EdgeTriggeredIOChunk)
	} else if options.EdgeTriggeredIO {
		options.EdgeTriggeredIOChunk = 1 << 20 // 1MB
	}

	rbc := options.ReadBufferCap
	switch {
	case rbc <= 0:
		options.ReadBufferCap = MaxStreamBufferCap
	case rbc <= ring.DefaultBufferSize:
		options.ReadBufferCap = ring.DefaultBufferSize
	default:
		options.ReadBufferCap = math.CeilToPowerOfTwo(rbc)
	}
	wbc := options.WriteBufferCap
	switch {
	case wbc <= 0:
		options.WriteBufferCap = MaxStreamBufferCap
	case wbc <= ring.DefaultBufferSize:
		options.WriteBufferCap = ring.DefaultBufferSize
	default:
		options.WriteBufferCap = math.CeilToPowerOfTwo(wbc)
	}

	var hasUDP, hasUnix bool
	for _, addr := range addrs {
		proto, _, err := parseProtoAddr(addr)
		if err != nil {
			return nil, nil, err
		}
		hasUDP = hasUDP || strings.HasPrefix(proto, "udp")
		hasUnix = hasUnix || proto == "unix"
	}

	goos := runtime.GOOS
	if options.ReusePort &&
		(options.Multicore || options.NumEventLoop > 1) &&
		(goos != "linux" && goos != "dragonfly" && goos != "freebsd") {
		options.ReusePort = false
	}

	if options.ReusePort && hasUnix {
		options.ReusePort = false
	}

	if hasUDP {
		options.ReusePort = true
		options.EdgeTriggeredIO = false
	}

	listeners := make([]*listener, len(addrs))
	for i, a := range addrs {
		proto, addr, err := parseProtoAddr(a)
		if err != nil {
			return nil, nil, err
		}
		ln, err := initListener(proto, addr, options)
		if err != nil {
			return nil, nil, err
		}
		listeners[i] = ln
	}

	return listeners, options, nil
}

// Run starts handling events on the specified address.
func Run(eventHandler EventHandler, protoAddr string, opts ...Option) error {
	listeners, options, err := createListeners([]string{protoAddr}, opts...)
	if err != nil {
		return err
	}
	defer func() {
		for _, ln := range listeners {
			ln.close()
		}
		logging.Cleanup()
	}()
	return run(eventHandler, listeners, options, []string{protoAddr})
}

// Rotate is like Run but accepts multiple network addresses.
func Rotate(eventHandler EventHandler, addrs []string, opts ...Option) error {
	listeners, options, err := createListeners(addrs, opts...)
	if err != nil {
		return err
	}
	defer func() {
		for _, ln := range listeners {
			ln.close()
		}
		logging.Cleanup()
	}()
	return run(eventHandler, listeners, options, addrs)
}

var (
	allEngines sync.Map

	// shutdownPollInterval is how often we poll to check whether engine has been shut down during Stop().
	shutdownPollInterval = 500 * time.Millisecond
)

// Stop gracefully shuts down the engine without interrupting any active event-loops.
//
// Deprecated: The global Stop only shuts down the last registered Engine with the same
// protocol and IP:Port. If you invoke gnet.Run multiple times with the same address
// (using WithReuseAddr/WithReusePort), previous engines are leaked in allEngines.
// Use Engine.Stop instead.
//
// FIX S-3: The previous implementation had two bugs:
//  1. When the engine was not found in allEngines, it returned ErrEngineInShutdown,
//     which is semantically wrong (the engine is simply not found, not shut down).
//     Fixed to return ErrEmptyEngine to accurately describe the situation.
//  2. It called eng.shutdown(nil) and then immediately checked eng.isShutdown(),
//     which is a race: shutdown() only cancels the context, while isShutdown() is
//     set to true only after eng.stop() completes — so the check always returned
//     false and the function incorrectly fell through to the poll loop regardless.
//     Fixed by removing the premature isShutdown() check entirely and letting the
//     poll loop below handle the correct terminal state.
//
// FIX S-4: If two goroutines call gnet.Run with the same address concurrently,
// the second Store overwrites the first entry in allEngines without shutting down
// the first engine — causing a goroutine and fd leak. We now use LoadOrStore to
// detect this condition and log a warning, guiding users toward Engine.Stop.
func Stop(ctx context.Context, protoAddr string) error {
	s, ok := allEngines.Load(protoAddr)
	if !ok {
		// FIX S-3 bug 1: was ErrEngineInShutdown, but the engine simply does not
		// exist in the map.  ErrEmptyEngine is the closest existing error.
		return errorx.ErrEmptyEngine
	}
	eng := s.(*engine)

	// FIX S-3 bug 2: removed the premature isShutdown() check that always
	// returned false immediately after calling shutdown().
	eng.shutdown(nil)
	defer allEngines.Delete(protoAddr)

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()
	for {
		if eng.isShutdown() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func parseProtoAddr(protoAddr string) (string, string, error) {
	// Percent-encode "%" in the address to avoid url.Parse error.
	protoAddr = strings.ReplaceAll(protoAddr, "%", "%25")

	if runtime.GOOS == "windows" {
		if strings.HasPrefix(protoAddr, "unix://") {
			parts := strings.SplitN(protoAddr, "://", 2)
			if parts[1] == "" {
				return "", "", errorx.ErrInvalidNetworkAddress
			}
			return parts[0], parts[1], nil
		}
	}

	u, err := url.Parse(protoAddr)
	if err != nil {
		return "", "", err
	}

	switch u.Scheme {
	case "":
		return "", "", errorx.ErrInvalidNetworkAddress
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		if u.Host == "" || u.Path != "" {
			return "", "", errorx.ErrInvalidNetworkAddress
		}
		return u.Scheme, u.Host, nil
	case "unix":
		hostPath := path.Join(u.Host, u.Path)
		if hostPath == "" {
			return "", "", errorx.ErrInvalidNetworkAddress
		}
		return u.Scheme, hostPath, nil
	default:
		return "", "", errorx.ErrUnsupportedProtocol
	}
}

func determineEventLoops(opts *Options) int {
	numEventLoop := 1
	if opts.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if opts.NumEventLoop > 0 {
		numEventLoop = opts.NumEventLoop
	}
	if numEventLoop > gfd.EventLoopIndexMax {
		numEventLoop = gfd.EventLoopIndexMax
	}
	return numEventLoop
}
