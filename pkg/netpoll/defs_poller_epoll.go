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

//go:build linux

package netpoll

import "golang.org/x/sys/unix"

// IOFlags represents the flags of IO events.
type IOFlags = uint16

// IOEvent is the integer type of I/O events on Linux.
type IOEvent = uint32

const (
	// InitPollEventsCap represents the initial capacity of poller event-list.
	InitPollEventsCap = 128
	// MaxPollEventsCap is the maximum limitation of events that the poller can process.
	MaxPollEventsCap = 1024
	// MinPollEventsCap is the minimum limitation of events that the poller can process.
	MinPollEventsCap = 32
	// MaxAsyncTasksAtOneTime is the maximum amount of asynchronous tasks that the event-loop will process at one time.
	MaxAsyncTasksAtOneTime = 256
	// ReadEvents represents readable events that are polled by epoll.
	//
	// FIX L-12: EPOLLPRI is a priority (out-of-band) event. Nothing here enables
	// SO_OOBINLINE or SO_SELECT_ERR_QUEUE, so the kernel does not put TCP urgent
	// data on the error queue these sockets read from and no EPOLLPRI is ever
	// generated for them; the bit is inert today. It is kept because it is what
	// upstream requests, but the coupling is implicit: enabling either socket
	// option later would start surfacing EPOLLPRI, and because IsReadEvent tests
	// ReadEvents, a level-triggered EPOLLPRI that nothing consumes would re-arm
	// every iteration — a read event that read(2) cannot clear. Remove the bit at
	// the same time as adding such an option, not after.
	ReadEvents = unix.EPOLLIN | unix.EPOLLPRI
	// WriteEvents represents writeable events that are polled by epoll.
	WriteEvents = unix.EPOLLOUT
	// ReadWriteEvents represents both readable and writeable events.
	ReadWriteEvents = ReadEvents | WriteEvents
	// ErrEvents represents exceptional events that occurred.
	//
	// FIX L-12: EPOLLRDHUP is deliberately absent. It is requested in edge-triggered
	// mode (see AddRead/ModReadWrite), and on a peer half-close the kernel reports it
	// together with EPOLLIN — because tcp_poll sets both when the receive side is
	// shut down. Adding it here would therefore classify an ordinary orderly
	// shutdown as an error event and tear the connection down before the data the
	// peer sent before closing was read. The read path already turns that same
	// condition into io.EOF when read(2) returns 0, which is where the close belongs.
	ErrEvents = unix.EPOLLERR | unix.EPOLLHUP
)

// IsReadEvent checks if the event is a read event.
func IsReadEvent(event IOEvent) bool {
	return event&ReadEvents != 0
}

// IsWriteEvent checks if the event is a write event.
func IsWriteEvent(event IOEvent) bool {
	return event&WriteEvents != 0
}

// IsErrorEvent checks if the event is an error event.
func IsErrorEvent(event IOEvent, _ IOFlags) bool {
	return event&ErrEvents != 0
}

type eventList struct {
	size   int
	events []epollevent
}

func newEventList(size int) *eventList {
	return &eventList{size, make([]epollevent, size)}
}

func (el *eventList) expand() {
	if newSize := el.size << 1; newSize <= MaxPollEventsCap {
		el.size = newSize
		el.events = make([]epollevent, newSize)
	}
}

func (el *eventList) shrink() {
	if newSize := el.size >> 1; newSize >= MinPollEventsCap {
		el.size = newSize
		el.events = make([]epollevent, newSize)
	}
}
