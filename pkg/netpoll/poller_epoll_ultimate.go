// Copyright (c) 2021 The Gnet Authors. All rights reserved.
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

//go:build linux && poll_opt

package netpoll

import (
	"errors"
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
	// Polling must return before Close; concurrent Trigger calls are synchronized.
	lifecycle sync.RWMutex
	fd                          int             // epoll fd
	epa                         *PollAttachment // PollAttachment for waking events
	efdBuf                      []byte          // efd buffer to read an 8-byte integer
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
	var efd int
	if efd, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC); err != nil {
		// FIX H-2/M-8: this used to call poller.Close(), which dereferences
		// poller.epa — still nil here, since it is assigned below — and panicked
		// instead of returning the eventfd error.  Only the epoll descriptor
		// exists yet, so close just that one.
		_ = unix.Close(poller.fd)
		poller = nil
		err = os.NewSyscallError("eventfd", err)
		return
	}
	poller.efdBuf = make([]byte, 8)
	poller.epa = &PollAttachment{FD: efd}
	if err = poller.AddRead(poller.epa, true); err != nil {
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
// Close is idempotent, and tolerates the partially initialised poller that
// OpenPoller hands to it when a step of the setup fails: epa is nil until the
// eventfd has been created, and fd/efa carry -1 once the poller is closed.
func (p *Poller) Close() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	// FIX M-11: the -1 sentinel doubles as the "closed" flag that Trigger checks.
	if p.fd < 0 {
		return nil
	}
	if p.epa != nil {
		_ = unix.Close(p.epa.FD)
		p.epa = nil
	}
	err := os.NewSyscallError("close", unix.Close(p.fd))
	p.fd = -1
	return err
}

// Make the endianness of bytes compatible with more linux OSs under different processor-architectures,
// according to http://man7.org/linux/man-pages/man2/eventfd.2.html.
var (
	u uint64 = 1
	b        = (*(*[8]byte)(unsafe.Pointer(&u)))[:]
)

// Trigger enqueues task and wakes up the poller to process pending tasks.
// By default, any incoming task will enqueued into urgentAsyncTaskQueue
// before the threshold of high-priority events is reached. When it happens,
// any asks other than high-priority tasks will be shunted to asyncTaskQueue.
//
// Note that asyncTaskQueue is a queue of low-priority whose size may grow large and tasks in it may backlog.
func (p *Poller) Trigger(priority queue.EventPriority, fn queue.Func, param any) (err error) {
	p.lifecycle.RLock()
	defer p.lifecycle.RUnlock()
	// FIX M-11: after Close the descriptors are -1 and their numbers may already
	// belong to unrelated files opened by the process.  Refuse the task up front —
	// before taking one out of the task pool, which would otherwise never be
	// returned to it.
	if p.fd < 0 {
		return errorx.ErrPollerClosed
	}
	task := queue.GetTask()
	task.Exec, task.Param = fn, param
	if priority > queue.HighPriority && p.urgentAsyncTaskQueue.Length() >= p.highPriorityEventsThreshold {
		p.asyncTaskQueue.Enqueue(task)
	} else {
		// There might be some low-priority tasks overflowing into urgentAsyncTaskQueue in a flash,
		// but that's tolerable because it ought to be a rare case.
		p.urgentAsyncTaskQueue.Enqueue(task)
	}
	if atomic.CompareAndSwapInt32(&p.wakeupCall, 0, 1) {
		for {
			_, err = unix.Write(p.epa.FD, b)
			if err == unix.EAGAIN {
				_, _ = unix.Read(p.epa.FD, p.efdBuf)
				continue
			}
			break
		}
	}
	return os.NewSyscallError("write", err)
}

// Polling blocks the current goroutine, monitoring the registered file descriptors and waiting for network I/O.
// When I/O occurs on any of the file descriptors, the provided callback function is invoked.
func (p *Poller) Polling() error {
	el := newEventList(InitPollEventsCap)
	var doChores bool

	msec := -1
	for {
		n, err := epollWait(p.fd, el.events, msec)
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
			pollAttachment := restorePollAttachment(unsafe.Pointer(&ev.data))
			if pollAttachment.FD == p.epa.FD { // poller is awakened to run tasks in queues.
				doChores = true
			} else {
				err = pollAttachment.Callback(pollAttachment.FD, ev.events, 0)
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
					_, err = unix.Write(p.epa.FD, b)
					if err == unix.EAGAIN {
						_, _ = unix.Read(p.epa.FD, p.efdBuf)
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

// AddReadWrite registers the given file descriptor with readable and writable events to the poller.
func (p *Poller) AddReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadWriteEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl add", epollCtl(p.fd, unix.EPOLL_CTL_ADD, pa.FD, &ev))
}

// AddRead registers the given file descriptor with readable event to the poller.
func (p *Poller) AddRead(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl add", epollCtl(p.fd, unix.EPOLL_CTL_ADD, pa.FD, &ev))
}

// AddWrite registers the given file descriptor with writable event to the poller.
func (p *Poller) AddWrite(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = WriteEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl add", epollCtl(p.fd, unix.EPOLL_CTL_ADD, pa.FD, &ev))
}

// ModRead modifies the given file descriptor with readable event in the poller.
func (p *Poller) ModRead(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl mod", epollCtl(p.fd, unix.EPOLL_CTL_MOD, pa.FD, &ev))
}

// ModReadWrite modifies the given file descriptor with readable and writable events in the poller.
func (p *Poller) ModReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadWriteEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl mod", epollCtl(p.fd, unix.EPOLL_CTL_MOD, pa.FD, &ev))
}

// Delete removes the given file descriptor from the poller.
func (p *Poller) Delete(fd int) error {
	return os.NewSyscallError("epoll_ctl del", epollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, nil))
}
