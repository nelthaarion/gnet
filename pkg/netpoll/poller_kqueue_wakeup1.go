//  Copyright (c) 2024 The Gnet Authors. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//go:build netbsd || openbsd

package netpoll

import (
	"golang.org/x/sys/unix"

	"github.com/nelthaarion/gnet/v2/pkg/logging"
)

// TODO(panjf2000): NetBSD didn't implement EVFILT_USER for user-established events
// until NetBSD 10.0, check out https://www.netbsd.org/releases/formal-10/NetBSD-10.0.html
// Therefore we use the pipe to wake up the kevent on NetBSD at this point. Get back here
// and switch to EVFILT_USER when we bump up the minimal requirement of NetBSD to 10.0.
// Alternatively, maybe we can use EVFILT_USER on the NetBSD by checking the kernel version
// via uname(3) and fall back to the pipe if the kernel version is older than 10.0.

func (p *Poller) addWakeupEvent() error {
	p.pipe = make([]int, 2)
	if err := unix.Pipe2(p.pipe[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		// FIX: this used to be logging.Fatalf, which exits the whole process from
		// inside a library: an engine that cannot get two file descriptors took the
		// host application down with it instead of failing Startup with an error.
		// A failed pipe2 leaves the slice untouched, i.e. holding two zeroes, so
		// drop it — otherwise Close would close file descriptor 0, the process's
		// stdin.
		p.pipe = nil
		return err
	}
	_, err := unix.Kevent(p.fd, []unix.Kevent_t{{
		// FIX: Kevent_t.Ident is uint32 on the 32-bit BSDs and uint64 on the 64-bit
		// ones, which is what the keventIdent alias exists for; a hard-coded
		// uint64(...) here failed to compile for netbsd/386 and openbsd/386.
		Ident:  keventIdent(p.pipe[0]),
		Filter: unix.EVFILT_READ,
		Flags:  unix.EV_ADD,
	}}, nil, nil)
	return err
}

func (p *Poller) wakePoller() error {
retry:
	_, err := unix.Write(p.pipe[1], []byte("x"))
	if err == nil || err == unix.EAGAIN {
		return nil
	}
	if err == unix.EINTR {
		goto retry
	}
	logging.Warnf("failed to write to the wakeup pipe: %v", err)
	return err
}

func (p *Poller) drainWakeupEvent() {
	var buf [8]byte
	_, _ = unix.Read(p.pipe[0], buf[:])
}

// isWakeupEvent reports whether ev is the event raised by wakePoller.
//
// On these platforms the wakeup is a pipe registered under its own file
// descriptor with the EVFILT_READ filter, so that descriptor is what identifies
// the event.  Polling used to recognise the wakeup by "Ident == 0" instead — the
// convention of the EVFILT_USER implementation used on the other BSDs — which
// never matched a pipe descriptor (they are 3 or above), so the wakeup was never
// drained: the queue never ran, wakeupCall was never cleared so only the first
// wakeup was ever written, and since the pipe stayed readable the poller span at
// 100% CPU in a non-blocking loop.
func (p *Poller) isWakeupEvent(ev *unix.Kevent_t) bool {
	return len(p.pipe) == 2 && ev.Filter == unix.EVFILT_READ && int(ev.Ident) == p.pipe[0]
}
