// Copyright (c) 2023 The Gnet Authors. All rights reserved.
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

package gnet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
	bbPool "github.com/nelthaarion/gnet/v2/pkg/pool/bytebuffer"
	bsPool "github.com/nelthaarion/gnet/v2/pkg/pool/byteslice"
	"github.com/nelthaarion/gnet/v2/pkg/pool/goroutine"
)

// maxUDPDatagramSize is the largest payload a UDP datagram can carry.
const maxUDPDatagramSize = 0x10000

type eventloop struct {
	submitMu     sync.RWMutex
	stopped      bool
	ch           chan any           // channel for event-loop
	idx          int                // index of event-loop in event-loops
	eng          *engine            // engine in loop
	connCount    int32              // number of active connections in event-loop
	connections  map[*conn]struct{} // TCP connection map: fd -> conn
	eventHandler EventHandler       // user eventHandler
}

func (el *eventloop) Register(ctx context.Context, addr net.Addr) (<-chan RegisteredResult, error) {
	if el.eng.isShutdown() {
		return nil, errorx.ErrEngineInShutdown
	}
	if addr == nil {
		return nil, errorx.ErrInvalidNetworkAddress
	}
	return el.enroll(nil, addr, FromContext(ctx))
}

func (el *eventloop) Enroll(ctx context.Context, c net.Conn) (<-chan RegisteredResult, error) {
	if el.eng.isShutdown() {
		return nil, errorx.ErrEngineInShutdown
	}
	if c == nil {
		return nil, errorx.ErrInvalidNetConn
	}
	return el.enroll(c, c.RemoteAddr(), FromContext(ctx))
}

func (el *eventloop) Execute(ctx context.Context, runnable Runnable) error {
	if el.eng.isShutdown() {
		return errorx.ErrEngineInShutdown
	}
	if runnable == nil {
		return errorx.ErrNilRunnable
	}
	return el.submit(func() error {
		if el.eng.beingShutdown.Load() {
			return errorx.ErrEngineInShutdown
		}
		return runnable.Run(ctx)
	})
}

func (el *eventloop) Schedule(context.Context, Runnable, time.Duration) error {
	return errorx.ErrUnsupportedOp
}

func (el *eventloop) Close(c Conn) error {
	return el.close(c.(*conn), nil)
}

func (el *eventloop) getLogger() logging.Logger {
	return el.eng.opts.Logger
}

func (el *eventloop) enroll(c net.Conn, addr net.Addr, ctx any) (resCh chan RegisteredResult, err error) {
	resCh = make(chan RegisteredResult, 1)
	err = goroutine.DefaultWorkerPool.Submit(func() {
		defer close(resCh)

		var err error
		if c == nil {
			if c, err = net.Dial(addr.Network(), addr.String()); err != nil {
				resCh <- RegisteredResult{Err: err}
				return
			}
		}

		connOpened := make(chan error, 1)
		udp := false
		var gc *conn
		switch addr.Network() {
		case "tcp", "tcp4", "tcp6", "unix":
			gc = newStreamConn(el, c, ctx)
		case "udp", "udp4", "udp6":
			udp = true
			gc = newUDPConn(el, nil, c, c.LocalAddr(), c.RemoteAddr(), ctx)
		default:
			// This branch used to fall through with gc left nil and no openConn
			// sent, so the `<-connOpened` below waited on a callback nothing would
			// ever run — a permanent hang of the calling goroutine.  It is reachable
			// whenever net.Dial understands a network this switch does not, such as
			// "unixpacket".
			_ = c.Close()
			resCh <- RegisteredResult{Err: errorx.ErrUnsupportedProtocol}
			return
		}

		// The reader has to start after the connection is published: it feeds
		// inbound data back as *tcpConn/*udpConn, and read() drops those for a
		// connection the loop does not know yet.
		if err = el.enqueue(&openConn{c: gc, cb: func(err error) { connOpened <- err }}); err != nil {
			_ = c.Close()
			gc.release()
			resCh <- RegisteredResult{Err: err}
			return
		}
		if err = <-connOpened; err != nil {
			resCh <- RegisteredResult{Err: err}
			return
		}
		if err = el.readLoop(c, gc, ctx, udp); err != nil {
			_ = el.enqueue(&netErr{gc, err})
			resCh <- RegisteredResult{Err: err}
			return
		}
		resCh <- RegisteredResult{Conn: gc}
	})
	return
}

// readLoop pumps data from nc into the event-loop until reading fails.
//
// FIX M-3: each reader used to own a `var buffer [0x10000]byte`, i.e. 64KB of its
// own for every connection regardless of what WithReadBufferCap asked for, resident
// for as long as the connection lived.  The buffer now comes from the shared slice
// pool, sized by the option for streams — a UDP socket cannot shrink it, since a
// datagram may legitimately be up to 65507 bytes and a smaller buffer would
// truncate it silently — and is returned to the pool when the reader stops.
//
// FIX M-4: the error from submitting the reader is returned rather than dropped, so
// a connection whose reader never started is reported to the caller instead of
// staying open and unread.
//
// ctx is the user context recorded on the connection; it is only used to build the
// fresh per-datagram connections of the UDP case, matching what the callers did
// inline before.
func (el *eventloop) readLoop(nc net.Conn, c *conn, ctx any, udp bool) error {
	bufCap := el.eng.opts.ReadBufferCap
	if udp {
		bufCap = maxUDPDatagramSize
	} else if bufCap <= 0 {
		bufCap = MaxStreamBufferCap
	}
	err := goroutine.DefaultWorkerPool.Submit(func() {
		buffer := bsPool.Get(bufCap)
		defer bsPool.Put(buffer)
		for {
			n, readErr := nc.Read(buffer)
			if n > 0 || (udp && readErr == nil) {
				if udp {
					uc := newUDPConn(el, nil, nc, nc.LocalAddr(), nc.RemoteAddr(), ctx)
					if err := el.enqueue(packUDPConn(uc, buffer[:n])); err != nil {
						uc.release()
						return
					}
				} else {
					tc := packTCPConn(c, buffer[:n])
					if err := el.enqueue(tc); err != nil {
						bbPool.Put(tc.b)
						return
					}
				}
			}
			if readErr != nil {
				_ = el.enqueue(&netErr{c, readErr})
				return
			}
		}
	})
	return err
}

func (el *eventloop) incConn(delta int32) {
	atomic.AddInt32(&el.connCount, delta)
}

func (el *eventloop) countConn() int32 {
	return atomic.LoadInt32(&el.connCount)
}

func (el *eventloop) run() (err error) {
	defer func() {
		el.eng.shutdown(err)
		el.submitMu.Lock()
		el.stopped = true
		el.submitMu.Unlock()
		for c := range el.connections {
			_ = el.close(c, nil)
		}
		el.finishPending()
	}()

	if el.eng.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	for {
		var i any
		select {
		case <-el.eng.concurrency.ctx.Done():
			return nil
		case i = <-el.ch:
		}
		switch v := i.(type) {
		case error:
			err = v
		case *netErr:
			err = el.close(v.c, v.err)
		case *openConn:
			err = el.open(v)
		case *tcpConn:
			err = el.read(unpackTCPConn(v))
		case *udpConn:
			err = el.readUDP(v.c)
		case func() error:
			err = v()
		}

		if errors.Is(err, errorx.ErrEngineShutdown) {
			el.getLogger().Debugf("event-loop(%d) is exiting in terms of the demand from user, %v", el.idx, err)
			break
		} else if err != nil {
			el.getLogger().Debugf("event-loop(%d) got a nonlethal error: %v", el.idx, err)
		}
	}

	return nil
}

func (el *eventloop) open(oc *openConn) (err error) {
	if oc.cb != nil {
		defer func() { oc.cb(err) }()
	}

	c := oc.c
	el.connections[c] = struct{}{}
	el.incConn(1)

	out, action := el.eventHandler.OnOpen(c)
	if out != nil {
		if _, err := c.Write(out); err != nil {
			_ = el.close(c, err)
			return err
		}
	}

	return el.handleAction(c, action)
}

func (el *eventloop) read(c *conn) error {
	if _, ok := el.connections[c]; !ok {
		return nil // ignore stale wakes.
	}
	action := el.eventHandler.OnTraffic(c)
	switch action {
	case None:
	case Close:
		return el.close(c, nil)
	case Shutdown:
		return errorx.ErrEngineShutdown
	}
	_, _ = c.inboundBuffer.Write(c.buffer.B)
	c.buffer.Reset()

	return nil
}

func (el *eventloop) readUDP(c *conn) error {
	defer c.release()
	if el.eventHandler.OnTraffic(c) == Shutdown {
		return errorx.ErrEngineShutdown
	}
	return nil
}

func (el *eventloop) ticker(ctx context.Context) {
	if el == nil {
		return
	}
	var (
		action Action
		delay  time.Duration
		timer  *time.Timer
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var shutdown bool
	for {
		delay, action = el.eventHandler.OnTick()
		switch action {
		case None, Close:
		case Shutdown:
			if !shutdown {
				shutdown = true
				el.eng.shutdown(errorx.ErrEngineShutdown)
				el.getLogger().Debugf("stopping ticker in event-loop(%d) from Tick()", el.idx)
			}
		}
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		select {
		case <-ctx.Done():
			el.getLogger().Debugf("stopping ticker in event-loop(%d) from Server, error:%v", el.idx, ctx.Err())
			return
		case <-timer.C:
		}
	}
}

func (el *eventloop) wake(c *conn) error {
	if _, ok := el.connections[c]; !ok {
		return nil // ignore stale wakes.
	}
	action := el.eventHandler.OnTraffic(c)
	return el.handleAction(c, action)
}

func (el *eventloop) close(c *conn, err error) error {
	if _, ok := el.connections[c]; c.rawConn == nil || !ok {
		return nil // ignore stale wakes.
	}

	delete(el.connections, c)
	el.incConn(-1)
	action := el.eventHandler.OnClose(c, err)
	err = c.rawConn.Close()
	c.release()
	if err != nil {
		return fmt.Errorf("failed to close connection=%s in event-loop(%d): %v", c.remoteAddr, el.idx, err)
	}

	return el.handleAction(c, action)
}

func (el *eventloop) handleAction(c *conn, action Action) error {
	switch action {
	case None:
		return nil
	case Close:
		return el.close(c, nil)
	case Shutdown:
		return errorx.ErrEngineShutdown
	default:
		return nil
	}
}
