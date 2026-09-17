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
	"net"
	"os"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/windows"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
)

type listener struct {
	openOnce, closeOnce sync.Once
	network             string
	address             string
	lc                  *net.ListenConfig
	ln                  net.Listener
	pc                  net.PacketConn
	addr                net.Addr
}

func (l *listener) dup() (int, error) {
	if l.ln == nil && l.pc == nil {
		return -1, errorx.ErrUnsupportedOp
	}

	var (
		sc syscall.Conn
		ok bool
	)
	if l.ln != nil {
		sc, ok = l.ln.(syscall.Conn)
	} else {
		sc, ok = l.pc.(syscall.Conn)
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

func (l *listener) open() (err error) {
	l.openOnce.Do(func() {
		switch l.network {
		case "udp", "udp4", "udp6":
			if l.pc, err = l.lc.ListenPacket(context.Background(), l.network, l.address); err == nil {
				l.addr = l.pc.LocalAddr()
			}
		case "unix":
			_ = os.Remove(l.address)
			fallthrough
		case "tcp", "tcp4", "tcp6":
			if l.ln, err = l.lc.Listen(context.Background(), l.network, l.address); err == nil {
				l.addr = l.ln.Addr()
			}
		default:
			err = errorx.ErrUnsupportedProtocol
		}
	})
	return
}

func (l *listener) close() {
	l.closeOnce.Do(func() {
		// A listener whose open() failed — an unsupported protocol, or an address
		// already in use — has neither field set, and the old code fell through to
		// l.ln.Close() on the nil interface, panicking while the engine was shutting
		// down (the unix listener guards on its fd for the same reason).
		if l.pc != nil {
			logging.Error(os.NewSyscallError("close", l.pc.Close()))
			l.pc = nil
			return
		}
		if l.ln != nil {
			logging.Error(os.NewSyscallError("close", l.ln.Close()))
			l.ln = nil
		}
	})
}

func initListener(network, addr string, options *Options) (*listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			// Every setsockopt below used to be ignored, so a listener could come up
			// with none of the requested socket options applied and no error to show
			// for it — ReuseAddr silently not set, or a receive buffer of whatever
			// the stack chose. Collect the first failure and fail the listen, which is
			// what the unix path does by returning them from socket.TCPSocket.
			var sockOptErr error
			controlErr := c.Control(func(fd uintptr) {
				h := windows.Handle(fd)
				set := func(err error) {
					if sockOptErr == nil {
						sockOptErr = err
					}
				}
				if network != "unix" && (options.ReuseAddr || options.ReusePort) {
					set(windows.SetsockoptInt(h, windows.SOL_SOCKET, windows.SO_REUSEADDR, 1))
				}
				// TCP_NODELAY is only meaningful on a stream socket: on a UDP socket
				// the call fails with WSAENOPROTOOPT. It used to be attempted anyway
				// and the error swallowed, which was invisible until the errors above
				// started being reported — at which point every UDP listener failed to
				// start. The unix path guards this the same way.
				if options.TCPNoDelay == TCPNoDelay && strings.HasPrefix(network, "tcp") {
					set(windows.SetsockoptInt(h, windows.IPPROTO_TCP, windows.TCP_NODELAY, 1))
				}
				if options.SocketRecvBuffer > 0 {
					set(windows.SetsockoptInt(h, windows.SOL_SOCKET, windows.SO_RCVBUF, options.SocketRecvBuffer))
				}
				if options.SocketSendBuffer > 0 {
					set(windows.SetsockoptInt(h, windows.SOL_SOCKET, windows.SO_SNDBUF, options.SocketSendBuffer))
				}
			})
			if controlErr != nil {
				return controlErr
			}
			return sockOptErr
		},
		KeepAlive: options.TCPKeepAlive,
	}

	l := listener{network: network, address: addr, lc: &lc}

	return &l, l.open()
}
