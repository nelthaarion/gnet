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

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package netpoll

import "github.com/nelthaarion/gnet/v2/pkg/queue"

// PollEventHandler is the callback for I/O events notified by the poller.
type PollEventHandler func(int, IOEvent, IOFlags) error

// PollAttachment is the user data which is about to be stored in "void *ptr" of epoll_data or "void *udata" of kevent.
//
// Lifetime contract, which matters under the poll_opt build tags: there the kernel
// is handed a raw pointer to the PollAttachment, and the Go garbage collector cannot
// see it — it scans Go memory, not the event registration held inside the kernel.
// The pointer is taken with unsafe.Pointer, which does not keep the object alive on
// its own.  The caller must therefore
//
//   - keep a reachable reference to the PollAttachment for as long as its file
//     descriptor is registered, and
//   - not copy or replace it in the meantime, since the poller will restore whatever
//     address it was given when the event arrives.
//
// Dropping the last reference while the descriptor is still registered leaves the
// kernel pointing at freed memory, and the next event for that descriptor makes the
// poller call a callback out of a dangling pointer.  Delete the descriptor from the
// poller before releasing the attachment.  The default (non-poll_opt) build carries
// the file descriptor number instead of a pointer and has no such requirement.
type PollAttachment struct {
	FD       int
	Callback PollEventHandler
}

// discardPendingTasks returns every task still sitting in the two task queues to
// the pool without executing it.
//
// It exists for the poller's shutdown path. A task that returns
// errorx.ErrEngineShutdown makes Polling return immediately, and the tasks queued
// behind it will never be run — so leaving them in the queues leaked one pooled
// *Task per pending task on every engine shutdown, because nothing else ever
// dequeues from a poller that has stopped polling. PutTask only clears the fields
// and hands the object to the sync.Pool; it does not run anything, so discarding
// unexecuted work this way is safe.
func discardPendingTasks(urgent, normal queue.AsyncTaskQueue) {
	for task := urgent.Dequeue(); task != nil; task = urgent.Dequeue() {
		queue.PutTask(task)
	}
	for task := normal.Dequeue(); task != nil; task = normal.Dequeue() {
		queue.PutTask(task)
	}
}
