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

package netpoll

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
	"github.com/nelthaarion/gnet/v2/pkg/queue"
)

// Poller represents a poller which is in charge of monitoring file-descriptors.
type Poller struct {
	// lifecycle protects task admission and wakeup descriptors against Close.
	// Polling must have returned before Close, as required by the engine lifecycle.
	lifecycle sync.RWMutex
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
		// FIX M-8: this used to call poller.Close(), which closes efd — still zero
		// at this point, so the poller closed file descriptor 0, i.e. stdin of the
		// hosting process.  Only the epoll descriptor exists yet, close that one.
		_ = unix.Close(poller.fd)
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
//
// Close is idempotent: it marks the poller as closed by setting both descriptors
// to -1, so closing twice is a no-op instead of a second close(2) on a descriptor
// number the runtime may have handed out again in the meantime.
func (p *Poller) Close() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	// FIX M-11: the -1 sentinel doubles as the "closed" flag that Trigger checks.
	if p.fd < 0 {
		return nil
	}
	// efd is only valid once OpenPoller got past unix.Eventfd; a bare Poller has
	// it at 0, which is stdin, so guard it the same way.
	if p.efd > 0 {
		_ = unix.Close(p.efd)
	}
	err := os.NewSyscallError("close", unix.Close(p.fd))
	p.fd, p.efd = -1, -1
	return err
}

// Make the endianness of bytes compatible with more linux OSs under different processor-architectures,
// according to http://man7.org/linux/man-pages/man2/eventfd.2.html.
var (
	u uint64 = 1
	b        = (*(*[8]byte)(unsafe.Pointer(&u)))[:]
)

// Trigger enqueues task and wakes up the poller to process pending tasks.
func (p *Poller) Trigger(priority queue.EventPriority, fn queue.Func, param any) (err error) {
	p.lifecycle.RLock()
	defer p.lifecycle.RUnlock()
	// FIX M-11: after Close the descriptors are -1 and, more importantly, their
	// numbers may already belong to unrelated files opened by the process.  Writing
	// to them would either fail or, worse, wake something that is not this poller,
	// so refuse the task up front — before taking one out of the task pool, which
	// would otherwise never be returned to it. The read lock protects both this
	// check and the subsequent wakeup syscall against descriptor closure.
	if p.fd < 0 {
		return errorx.ErrPollerClosed
	}
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
			// FIX L-1: ev.Fd is int32 — that is the width of the epoll_event struct
			// field on Linux. epollAdd (below) only ever installs a non-negative fd
			// that fits in int32, so int(ev.Fd) reproduces the registered fd exactly
			// and the comparison against the poller's own eventfd is meaningful.
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
					// FIX L-11: this task is as much a pooled object as any other and
					// used to be dropped on the way out — one leaked *Task per poller
					// shutdown. The tasks still queued are returned unexecuted for the
					// same reason: Polling is over, so nothing will ever dequeue them.
					queue.PutTask(task)
					discardPendingTasks(p.urgentAsyncTaskQueue, p.asyncTaskQueue)
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
					queue.PutTask(task)
					discardPendingTasks(p.urgentAsyncTaskQueue, p.asyncTaskQueue)
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

// epollAdd is the single helper that calls unix.EpollCtl(ADD/MOD) with an
// explicit range check on the fd.
//
// FIX L-1: this guard used to test only `pa.FD > math.MaxInt32`. That left the
// negative half unguarded, so a bogus fd such as -1 was written to the kernel as
// int32(-1) and, more importantly, round-tripped back on the read path as a
// negative int that can never equal a real fd — the event would then be routed to
// callback() for a connection the matrix does not hold. Both bounds are checked
// now, and the comparison is written as int64 so it means the same thing on every
// word size: as written before, on a 32-bit GOARCH `pa.FD > math.MaxInt32` was
// `int > MaxInt`, a constant-false expression the compiler cannot warn about
// because the constant is what makes it false.
//
// Note what this is and is not. It is a cheap consistency check on a value that
// gnet itself produced from accept(2)/socket(2); it is not a security boundary,
// and it is not the reason fds above 2^30 work — the kernel's own per-process fd
// limit (fs.nr_open, 2^20 by default) is what keeps the int32 epoll_data field
// from truncating in practice. The poll_opt build does not need the check at all
// because it stores a pointer in the epoll_data union instead of the fd.
func epollAdd(epfd int, op int, pa *PollAttachment, ev uint32) error {
	if pa.FD < 0 || int64(pa.FD) > math.MaxInt32 {
		return fmt.Errorf("epoll_ctl: fd %d is out of the int32 range that the "+
			"epoll_event struct can carry", pa.FD)
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
