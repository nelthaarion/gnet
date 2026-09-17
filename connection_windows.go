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
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/windows"

	"github.com/nelthaarion/gnet/v2/pkg/buffer/elastic"
	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	bbPool "github.com/nelthaarion/gnet/v2/pkg/pool/bytebuffer"
	bsPool "github.com/nelthaarion/gnet/v2/pkg/pool/byteslice"
)

type netErr struct {
	c   *conn
	err error
}

type tcpConn struct {
	c *conn
	b *bbPool.ByteBuffer
}

type udpConn struct {
	c *conn
}

type openConn struct {
	c  *conn
	cb func(error)
}

type conn struct {
	pc            net.PacketConn
	ctx           any                 // user-defined context
	safeCtx       atomic.Pointer[any] // safe user-defined context
	loop          *eventloop          // owner event-loop
	buffer        *bbPool.ByteBuffer  // reuse memory of inbound data as a temporary buffer
	cache         []byte              // temporary cache for the inbound data
	peekCaches    [][]byte            // allocated Peek results, valid until Discard/release
	rawConn       net.Conn            // original connection
	localAddr     net.Addr            // local server addr
	remoteAddr    net.Addr            // remote addr
	inboundBuffer elastic.RingBuffer  // buffer for data from the remote
}

func packTCPConn(c *conn, buf []byte) *tcpConn {
	b := bbPool.Get()
	_, _ = b.Write(buf)
	return &tcpConn{c: c, b: b}
}

func unpackTCPConn(tc *tcpConn) *conn {
	if tc.c.buffer == nil { // the connection has been closed
		// FIX L-9: this returned without releasing tc.b. packTCPConn took that buffer
		// from bbPool, so whichever branch is taken it has to go back — dropping it
		// here means the pool is drained by exactly the connections that close early,
		// which is the common case under load.
		bbPool.Put(tc.b)
		tc.b = nil
		return nil
	}
	_, _ = tc.c.buffer.Write(tc.b.B)
	bbPool.Put(tc.b)
	tc.b = nil
	return tc.c
}

func packUDPConn(c *conn, buf []byte) *udpConn {
	_, _ = c.buffer.Write(buf)
	return &udpConn{c}
}

func newStreamConn(el *eventloop, nc net.Conn, ctx any) (c *conn) {
	c = &conn{
		ctx:        ctx,
		loop:       el,
		buffer:     bbPool.Get(),
		rawConn:    nc,
		localAddr:  nc.LocalAddr(),
		remoteAddr: nc.RemoteAddr(),
	}
	c.SetSafeContext(ctx)
	return c
}

func (c *conn) release() {
	c.releasePeekCaches()
	c.ctx = nil
	c.safeCtx.Store(nil)
	c.localAddr = nil
	if c.rawConn != nil {
		c.rawConn = nil
		c.remoteAddr = nil
	}
	c.inboundBuffer.Done()

	// FIX P-8 (Windows): Next()/Peek() hand out a slice owned by bsPool and park it
	// in c.cache until Discard() returns it. A handler that never reaches Discard —
	// because the connection closed mid-protocol — left that slice to the GC, so the
	// pool leaked one block per such connection. Return it here, as the unix
	// implementation does.
	if len(c.cache) > 0 {
		bsPool.Put(c.cache)
		c.cache = nil
	}

	bbPool.Put(c.buffer)
	c.buffer = nil
}

func newUDPConn(el *eventloop, pc net.PacketConn, rc net.Conn, localAddr, remoteAddr net.Addr, ctx any) *conn {
	c := &conn{
		ctx:        ctx,
		pc:         pc,
		rawConn:    rc,
		loop:       el,
		buffer:     bbPool.Get(),
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
	}
	c.SetSafeContext(ctx)
	return c
}

// FIX M-5: release() sets c.buffer to nil, and InboundBuffered/WriteTo already
// tolerate that, but every other buffer access below dereferenced it and panicked.
// A connection handle is routinely kept by the caller past OnClose (in a map, or in
// a closed-over variable), so calling Read/Next/Peek/Discard on it is an ordinary
// mistake, not an impossible one. They now report the connection as closed instead,
// the same answer Write/SetLinger/... give.

func (c *conn) resetBuffer() {
	if c.buffer == nil { // the connection has been closed
		return
	}
	c.buffer.Reset()
	c.inboundBuffer.Reset()
	c.inboundBuffer.Done()
}

func (c *conn) Read(p []byte) (n int, err error) {
	if c.buffer == nil { // the connection has been closed
		return 0, net.ErrClosed
	}
	if c.inboundBuffer.IsEmpty() {
		n = copy(p, c.buffer.B)
		c.buffer.B = c.buffer.B[n:]
		if n == 0 && len(p) > 0 {
			err = io.ErrShortBuffer
		}
		return
	}
	n, _ = c.inboundBuffer.Read(p)
	if n == len(p) {
		return
	}
	m := copy(p[n:], c.buffer.B)
	n += m
	c.buffer.B = c.buffer.B[m:]
	return
}

func (c *conn) Next(n int) (buf []byte, err error) {
	if c.buffer == nil { // the connection has been closed
		return nil, net.ErrClosed
	}
	inBufferLen := c.inboundBuffer.Buffered()
	if totalLen := inBufferLen + c.buffer.Len(); n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if c.inboundBuffer.IsEmpty() {
		buf = c.buffer.B[:n]
		c.buffer.B = c.buffer.B[n:]
		return
	}

	// FIX P-2 (Windows): the slice obtained from bsPool was never returned to it, so
	// every Next() served from the inbound buffer leaked one pooled block. Keep the
	// same contract as the unix implementation: park the slice in c.cache, and let
	// the next Next()/Discard() or release() return it.
	if len(c.cache) > 0 {
		bsPool.Put(c.cache)
		c.cache = nil
	}
	buf = bsPool.Get(n)
	_, err = c.Read(buf)
	c.cache = buf
	return
}

func (c *conn) Peek(n int) (buf []byte, err error) {
	if c.buffer == nil { // the connection has been closed
		return nil, net.ErrClosed
	}
	inBufferLen := c.inboundBuffer.Buffered()
	if totalLen := inBufferLen + c.buffer.Len(); n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if c.inboundBuffer.IsEmpty() {
		return c.buffer.B[:n], err
	}
	head, tail := c.inboundBuffer.Peek(n)
	if len(head) == n {
		return head, err
	}
	buf = bsPool.Get(n)[:0]
	buf = append(buf, head...)
	buf = append(buf, tail...)
	if inBufferLen < n {
		buf = append(buf, c.buffer.B[:n-inBufferLen]...)
	}
	c.peekCaches = append(c.peekCaches, buf)
	return
}

func (c *conn) Discard(n int) (int, error) {
	c.releasePeekCaches()
	if len(c.cache) > 0 {
		bsPool.Put(c.cache)
		c.cache = nil
	}

	if c.buffer == nil { // the connection has been closed
		return 0, net.ErrClosed
	}

	inBufferLen := c.inboundBuffer.Buffered()
	if totalLen := inBufferLen + c.buffer.Len(); n >= totalLen || n <= 0 {
		c.resetBuffer()
		return totalLen, nil
	}

	if c.inboundBuffer.IsEmpty() {
		c.buffer.B = c.buffer.B[n:]
		return n, nil
	}

	discarded, _ := c.inboundBuffer.Discard(n)
	if discarded < inBufferLen {
		return discarded, nil
	}

	remaining := n - inBufferLen
	c.buffer.B = c.buffer.B[remaining:]
	return n, nil
}

// ── Windows write path: a documented limitation ───────────────────────────────
//
// FIX L-9: the Windows backend has no outbound buffering. Write/Writev call
// net.Conn.Write directly, on the event-loop goroutine, and block there until the
// kernel accepts every byte; OutboundBuffered always reports 0 and Flush is a
// no-op because there is nothing queued to flush.
//
// The consequence is head-of-line blocking: one connection whose peer stops
// reading occupies its event-loop for as long as the socket send buffer stays
// full, and every other connection on that loop waits behind it. There is also no
// backpressure signal — OutboundBuffered() cannot tell an application to slow
// down, because nothing is ever buffered.
//
// This is stated plainly rather than papered over, because it is a real
// behavioural difference from the unix build, where writes go through
// conn.outboundBuffer and Write returns as soon as the kernel takes what it will.
// Fixing it properly means giving each Windows connection an outbound buffer plus
// a serialized writer (otherwise two concurrent Writes could interleave on the
// wire), which is a redesign of this backend rather than a patch; it is left as
// that, deliberately. Applications that must not block a loop should size their
// writes to what the peer drains and treat OutboundBuffered() as always zero.

func (c *conn) Write(p []byte) (int, error) {
	if c.rawConn == nil && c.pc == nil {
		return 0, net.ErrClosed
	}
	if c.rawConn != nil {
		return c.rawConn.Write(p)
	}
	return c.pc.WriteTo(p, c.remoteAddr)
}

func (c *conn) SendTo(p []byte, addr net.Addr) (int, error) {
	if c.pc == nil {
		return 0, errorx.ErrUnsupportedOp
	}

	if addr == nil {
		return 0, errorx.ErrInvalidNetworkAddress
	}

	return c.pc.WriteTo(p, addr)
}

func (c *conn) Writev(bs [][]byte) (int, error) {
	if c.pc != nil { // not available for UDP
		return 0, errorx.ErrUnsupportedOp
	}

	if c.rawConn != nil {
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for i := range bs {
			_, _ = bb.Write(bs[i])
		}
		return c.rawConn.Write(bb.Bytes())
	}
	return 0, net.ErrClosed
}

func (c *conn) ReadFrom(r io.Reader) (int64, error) {
	if c.rawConn != nil {
		return io.Copy(c.rawConn, r)
	}
	return 0, net.ErrClosed
}

func (c *conn) WriteTo(w io.Writer) (n int64, err error) {
	if !c.inboundBuffer.IsEmpty() {
		if n, err = c.inboundBuffer.WriteTo(w); err != nil {
			return
		}
	}

	if c.buffer == nil || len(c.buffer.B) == 0 {
		return n, nil
	}
	m, writeErr := w.Write(c.buffer.B)
	if m < 0 || m > len(c.buffer.B) {
		panic("Conn.WriteTo: invalid Write count")
	}
	n += int64(m)
	c.buffer.B = c.buffer.B[m:]
	if writeErr == nil && len(c.buffer.B) > 0 {
		writeErr = io.ErrShortWrite
	}
	return n, writeErr
}

// Flush is a no-op on Windows: see "Windows write path" above. There is no
// outbound buffer for it to drain.
func (c *conn) Flush() error {
	return nil
}

func (c *conn) InboundBuffered() int {
	if c.buffer == nil {
		return 0
	}
	return c.inboundBuffer.Buffered() + c.buffer.Len()
}

// OutboundBuffered always returns 0 on Windows: see "Windows write path" above.
// Writes are synchronous, so no data is ever held by the library.
func (c *conn) OutboundBuffered() int {
	return 0
}

func (c *conn) Context() any         { return c.ctx }
func (c *conn) SetContext(ctx any)   { c.ctx = ctx }
func (c *conn) LocalAddr() net.Addr  { return c.localAddr }
func (c *conn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *conn) Fd() (fd int) {
	if c.rawConn == nil {
		return -1
	}

	// FIX L-9: this assertion used to be unchecked, so any net.Conn that does not
	// implement syscall.Conn — a user wrapper, a TLS conn, a test double passed to
	// Enroll — panicked inside the event-loop instead of reporting no descriptor.
	// Dup() right below has always checked it; do the same here.
	sc, ok := c.rawConn.(syscall.Conn)
	if !ok {
		return -1
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1
	}
	if err := rc.Control(func(i uintptr) {
		fd = int(i)
	}); err != nil {
		return -1
	}
	return
}

func (c *conn) Dup() (fd int, err error) {
	if c.rawConn == nil && c.pc == nil {
		return -1, net.ErrClosed
	}

	var (
		sc syscall.Conn
		ok bool
	)
	if c.rawConn != nil {
		sc, ok = c.rawConn.(syscall.Conn)
	} else {
		sc, ok = c.pc.(syscall.Conn)
	}

	if !ok {
		return -1, errors.New("failed to convert net.Conn to syscall.Conn")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1, errors.New("failed to get syscall.RawConn from net.Conn")
	}

	var dupHandle windows.Handle
	e := rc.Control(func(fd uintptr) {
		process := windows.CurrentProcess()
		err = windows.DuplicateHandle(
			process,
			windows.Handle(fd),
			process,
			&dupHandle,
			0,
			true,
			windows.DUPLICATE_SAME_ACCESS,
		)
	})
	if err != nil {
		return -1, err
	}
	if e != nil {
		return -1, e
	}

	return int(dupHandle), nil
}

func (c *conn) SetReadBuffer(bytes int) error {
	if c.rawConn == nil && c.pc == nil {
		return net.ErrClosed
	}

	if c.rawConn != nil {
		return c.rawConn.(interface{ SetReadBuffer(int) error }).SetReadBuffer(bytes)
	}
	return c.pc.(interface{ SetReadBuffer(int) error }).SetReadBuffer(bytes)
}

func (c *conn) SetWriteBuffer(bytes int) error {
	if c.rawConn == nil && c.pc == nil {
		return net.ErrClosed
	}
	if c.rawConn != nil {
		return c.rawConn.(interface{ SetWriteBuffer(int) error }).SetWriteBuffer(bytes)
	}
	return c.pc.(interface{ SetWriteBuffer(int) error }).SetWriteBuffer(bytes)
}

func (c *conn) SetLinger(sec int) error {
	if c.rawConn == nil {
		return net.ErrClosed
	}

	tc, ok := c.rawConn.(*net.TCPConn)
	if !ok {
		return errorx.ErrUnsupportedOp
	}
	return tc.SetLinger(sec)
}

func (c *conn) SetNoDelay(noDelay bool) error {
	if c.rawConn == nil {
		return net.ErrClosed
	}

	tc, ok := c.rawConn.(*net.TCPConn)
	if !ok {
		return errorx.ErrUnsupportedOp
	}
	return tc.SetNoDelay(noDelay)
}

func (c *conn) SetKeepAlivePeriod(d time.Duration) error {
	return c.SetKeepAlive(d > 0, d, d/5, 5)
}

func (c *conn) SetKeepAlive(enabled bool, idle, intvl time.Duration, cnt int) error {
	if c.rawConn == nil && c.pc == nil {
		return net.ErrClosed
	}

	if c.pc != nil {
		return errorx.ErrUnsupportedOp
	}

	tc, ok := c.rawConn.(*net.TCPConn)
	if !ok {
		return errorx.ErrUnsupportedOp
	}

	if enabled && (idle <= 0 || intvl <= 0 || cnt <= 0) {
		return errors.New("invalid time duration")
	}

	if err := tc.SetKeepAlive(enabled); err != nil {
		return err
	}

	if !enabled {
		return nil
	}

	if err := tc.SetKeepAlivePeriod(idle); err != nil {
		return err
	}

	if err := windows.SetsockoptInt(
		windows.Handle(c.Fd()),
		windows.IPPROTO_TCP,
		windows.TCP_KEEPINTVL,
		int(intvl.Seconds())); err != nil {
		return os.NewSyscallError("setsockopt", err)
	}

	if err := windows.SetsockoptInt(
		windows.Handle(c.Fd()),
		windows.IPPROTO_TCP,
		windows.TCP_KEEPCNT,
		cnt); err != nil {
		return os.NewSyscallError("setsockopt", err)
	}

	return nil
}

// Gfd return an uninitialized GFD which is not valid,
// this method is only implemented for compatibility, don't use it on Windows.
// func (c *conn) Gfd() gfd.GFD { return gfd.GFD{} }

func (c *conn) AsyncWrite(buf []byte, cb AsyncCallback) error {
	fn := func() error {
		_, err := c.Write(buf)
		if cb != nil {
			_ = cb(c, err)
		}
		return err
	}

	return c.loop.submit(fn)
}

func (c *conn) AsyncWritev(bs [][]byte, cb AsyncCallback) error {
	if c.pc != nil {
		return errorx.ErrUnsupportedOp
	}

	buf := bbPool.Get()
	for _, b := range bs {
		_, _ = buf.Write(b)
	}
	err := c.AsyncWrite(buf.Bytes(), func(c Conn, err error) error {
		defer bbPool.Put(buf)
		if cb == nil {
			return err
		}
		return cb(c, err)
	})
	if err != nil {
		bbPool.Put(buf)
	}
	return err
}

func (c *conn) Wake(cb AsyncCallback) (err error) {
	wakeFn := func() (err error) {
		err = c.loop.wake(c)
		if cb != nil {
			_ = cb(c, err)
		}
		return
	}

	return c.loop.submit(wakeFn)
}

func (c *conn) Close() (err error) {
	closeFn := func() error {
		return c.loop.close(c, nil)
	}

	return c.loop.submit(closeFn)
}

func (c *conn) CloseWithCallback(cb AsyncCallback) (err error) {
	closeFn := func() (err error) {
		err = c.loop.close(c, nil)
		if cb != nil {
			_ = cb(c, err)
		}
		return
	}

	return c.loop.submit(closeFn)
}

func (c *conn) EventLoop() EventLoop {
	return c.loop
}

func (*conn) SetDeadline(_ time.Time) error {
	return errorx.ErrUnsupportedOp
}

func (*conn) SetReadDeadline(_ time.Time) error {
	return errorx.ErrUnsupportedOp
}

func (*conn) SetWriteDeadline(_ time.Time) error {
	return errorx.ErrUnsupportedOp
}

func (c *conn) SafeContext() (ctx any) {
	if p := c.safeCtx.Load(); p != nil {
		return *p
	}
	return nil
}

func (c *conn) SetSafeContext(ctx any) {
	c.safeCtx.Store(&ctx)
}
