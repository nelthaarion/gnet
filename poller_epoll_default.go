// Copyright (c) 2019 Andy Pan
// Copyright (c) 2017 Joshua J Baker
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

//go:build linux && !poll_opt

package gnet

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
	"github.com/nelthaarion/gnet/v2/pkg/queue"
)

// Poller represents a poller which is in charge of monitoring file-descriptors.
type Poller struct {
	fd                          int    // epoll fd
	efd                         int    // eventfd
	efdBuf                      []byte // efd buffer to read an 8-byte integer
	wakeupCall                  int32
	asyncTaskQueue              queue.AsyncTaskQueue // queue with low priority
	urgentAsyncTaskQueue        queue.AsyncTaskQueue // queue with high priority
	highPriorityEventsThreshold int32                // threshold of high-priority events
}

// OpenPoller instantiates a poller.
func OpenPoller() (poller *Poller, err error) {
	poller = new(Poller)
	if poller.fd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
		poller = nil
		err = os.NewSyscallError("epoll_create1", err)
		return
	}
	if poller.efd, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC); err != nil {
		_ = poller.Close()
		poller = nil
		err = os.NewSyscallError("eventfd", err)
		return
	}
	poller.efdBuf = make([]byte, 8)
	if err = poller.AddRead(&PollAttachment{FD: poller.efd}, true); err != nil {
		_ = poller.Close()
		poller = nil
		return
	}
	poller.asyncTaskQueue = queue.NewLockFreeQueue()
	poller.urgentAsyncTaskQueue = queue.NewLockFreeQueue()
	poller.highPriorityEventsThreshold = MaxPollEventsCap
	return
}

// Close closes the poller.
func (p *Poller) Close() error {
	_ = unix.Close(p.efd)
	return os.NewSyscallError("close", unix.Close(p.fd))
}

// Make the endianness of bytes compatible with more linux OSs under different processor-architectures,
// according to http://man7.org/linux/man-pages/man2/eventfd.2.html.
var (
	u uint64 = 1
	b        = (*(*[8]byte)(unsafe.Pointer(&u)))[:]
)

// Trigger enqueues task and wakes up the poller to process pending tasks.
func (p *Poller) Trigger(priority queue.EventPriority, fn queue.Func, param any) (err error) {
	task := queue.GetTask()
	task.Exec, task.Param = fn, param
	if priority > queue.HighPriority && p.urgentAsyncTaskQueue.Length() >= p.highPriorityEventsThreshold {
		p.asyncTaskQueue.Enqueue(task)
	} else {
		p.urgentAsyncTaskQueue.Enqueue(task)
	}
	if atomic.CompareAndSwapInt32(&p.wakeupCall, 0, 1) {
		for {
			_, err = unix.Write(p.efd, b)
			if err == unix.EAGAIN {
				_, _ = unix.Read(p.efd, p.efdBuf)
				continue
			}
			break
		}
	}
	return os.NewSyscallError("write", err)
}

// Polling blocks the current goroutine, monitoring the registered file descriptors.
func (p *Poller) Polling(callback PollEventHandler) error {
	el := newEventList(InitPollEventsCap)
	var doChores bool

	msec := -1
	for {
		n, err := unix.EpollWait(p.fd, el.events, msec)
		if n == 0 || (n < 0 && err == unix.EINTR) {
			msec = -1
			runtime.Gosched()
			continue
		} else if err != nil {
			logging.Errorf("error occurs in epoll: %v", os.NewSyscallError("epoll_wait", err))
			return err
		}
		msec = 0

		for i := 0; i < n; i++ {
			ev := &el.events[i]
			// FIX S-1: ev.Fd is int32 (the EpollEvent struct field is int32 on Linux).
			// We store the fd in Fd only when it fits in int32 (see epollAdd below).
			// On the read path, convert back via int(ev.Fd) which sign-extends on
			// 64-bit; since we only store valid non-negative fds, this is safe.
			if fd := int(ev.Fd); fd == p.efd { // poller is awakened to run tasks in queues.
				doChores = true
			} else {
				err = callback(fd, ev.Events, 0)
				if errors.Is(err, errorx.ErrAcceptSocket) || errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
			}
		}

		if doChores {
			doChores = false
			task := p.urgentAsyncTaskQueue.Dequeue()
			for ; task != nil; task = p.urgentAsyncTaskQueue.Dequeue() {
				err = task.Exec(task.Param)
				if errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
				queue.PutTask(task)
			}
			for i := 0; i < MaxAsyncTasksAtOneTime; i++ {
				if task = p.asyncTaskQueue.Dequeue(); task == nil {
					break
				}
				err = task.Exec(task.Param)
				if errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
				queue.PutTask(task)
			}
			atomic.StoreInt32(&p.wakeupCall, 0)
			if (!p.asyncTaskQueue.IsEmpty() || !p.urgentAsyncTaskQueue.IsEmpty()) && atomic.CompareAndSwapInt32(&p.wakeupCall, 0, 1) {
				for {
					_, err = unix.Write(p.efd, b)
					if err == unix.EAGAIN {
						_, _ = unix.Read(p.efd, p.efdBuf)
						continue
					}
					if err != nil {
						logging.Errorf("failed to notify next round of event-loop for leftover tasks, %v", os.NewSyscallError("write", err))
					}
					break
				}
			}
		}

		if n == el.size {
			el.expand()
		} else if n < el.size>>1 {
			el.shrink()
		}
	}
}

// epollAdd is the single helper that calls unix.EpollCtl(ADD/MOD) with a
// safe int32 fd check.
//
// FIX S-1: The Linux epoll_event struct stores Fd as int32. On amd64 (and all
// 64-bit Linux platforms), file descriptors are non-negative ints that can in
// theory reach up to /proc/sys/fs/nr_open (default 1048576) or even higher
// with custom kernel configuration. In practice the kernel enforces a per-process
// limit well below MaxInt32, but we add an explicit guard to make the truncation
// visible and turn it into an error instead of silent misbehaviour.
//
// If pa.FD exceeds math.MaxInt32, we return an error pointing the operator to
// the poll_opt build tag, whose implementation uses a uintptr pointer in the
// epoll_data union instead of the Fd field, avoiding the int32 constraint.
func epollAdd(epfd int, op int, pa *PollAttachment, ev uint32) error {
	if pa.FD > math.MaxInt32 {
		// This should never happen on Linux with default kernel settings, but
		// if it does (e.g. after tens of millions of connections on a long-running
		// server with fd recycling disabled), the truncation would cause the wrong
		// connection to be woken up or the poller's eventfd to be falsely matched.
		// Fail loudly rather than silently corrupt I/O routing.
		return fmt.Errorf("epoll_ctl: fd %d exceeds int32 range; "+
			"rebuild with the 'poll_opt' build tag to use pointer-based epoll_data", pa.FD)
	}
	return os.NewSyscallError("epoll_ctl",
		unix.EpollCtl(epfd, op, pa.FD, &unix.EpollEvent{
			Fd:     int32(pa.FD), // safe: guarded above
			Events: ev,
		}))
}

// AddReadWrite registers the given file descriptor with readable and writable events to the poller.
func (p *Poller) AddReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	ev := uint32(ReadWriteEvents)
	if edgeTriggered {
		ev |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	return epollAdd(p.fd, unix.EPOLL_CTL_ADD, pa, ev)
}

// AddRead registers the given file descriptor with readable event to the poller.
func (p *Poller) AddRead(pa *PollAttachment, edgeTriggered bool) error {
	ev := uint32(ReadEvents)
	if edgeTriggered {
		ev |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	return epollAdd(p.fd, unix.EPOLL_CTL_ADD, pa, ev)
}

// AddWrite registers the given file descriptor with writable event to the poller.
func (p *Poller) AddWrite(pa *PollAttachment, edgeTriggered bool) error {
	ev := uint32(WriteEvents)
	if edgeTriggered {
		ev |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	return epollAdd(p.fd, unix.EPOLL_CTL_ADD, pa, ev)
}

// ModRead modifies the given file descriptor with readable event in the poller.
func (p *Poller) ModRead(pa *PollAttachment, edgeTriggered bool) error {
	ev := uint32(ReadEvents)
	if edgeTriggered {
		ev |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	return epollAdd(p.fd, unix.EPOLL_CTL_MOD, pa, ev)
}

// ModReadWrite modifies the given file descriptor with readable and writable events in the poller.
func (p *Poller) ModReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	ev := uint32(ReadWriteEvents)
	if edgeTriggered {
		ev |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	return epollAdd(p.fd, unix.EPOLL_CTL_MOD, pa, ev)
}

// Delete removes the given file descriptor from the poller.
func (p *Poller) Delete(fd int) error {
	return os.NewSyscallError("epoll_ctl del", unix.EpollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, nil))
}
