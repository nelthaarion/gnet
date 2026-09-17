// Copyright (c) 2025 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package netpoll_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/netpoll"
	"github.com/nelthaarion/gnet/v2/pkg/queue"
)

// pollingTimeout bounds every poller test. Without it a regression in the wakeup
// path does not fail the test — it hangs the package until the go test timeout,
// which reports a stack dump instead of the assertion that broke.
const pollingTimeout = 10 * time.Second

// runPoller starts Polling on its own goroutine and returns a channel carrying
// its result, so the caller can bound the wait.
func runPoller(t *testing.T, p *netpoll.Poller, pa *netpoll.PollAttachment) <-chan error {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- examplePoll(p, pa) }()
	return errc
}

// waitPoller blocks for the poller to stop, failing the test if it does not.
func waitPoller(t *testing.T, errc <-chan error) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(pollingTimeout):
		t.Fatalf("poller did not stop within %v", pollingTimeout)
		return nil
	}
}

// TestPollerRunsTriggeredTask covers the wakeup mechanism that AsyncWrite, Wake,
// CloseWithCallback and cross-loop registration all go through, and with it the
// failure modes around it: a poller that cannot be opened (the error paths in
// OpenPoller that used to panic or close fd 0) and a triggered task that is
// queued but never dequeued (the netbsd/openbsd identity mismatch).
//
// Nothing here touched a real socket, so it is the cheapest possible check that
// Trigger -> wakeup -> doChores -> Exec still forms a closed loop.
func TestPollerRunsTriggeredTask(t *testing.T) {
	p, err := netpoll.OpenPoller()
	if err != nil {
		t.Fatalf("OpenPoller() error: %v", err)
	}
	defer p.Close() //nolint:errcheck

	var ran atomic.Bool
	if err := p.Trigger(queue.HighPriority, func(any) error {
		ran.Store(true)
		return errorx.ErrEngineShutdown
	}, nil); err != nil {
		t.Fatalf("Trigger() error: %v", err)
	}

	// The attachment is a placeholder: this test only exercises the task queue,
	// and no descriptor is registered with the poller besides its own wakeup fd.
	err = waitPoller(t, runPoller(t, p, &netpoll.PollAttachment{}))

	if !ran.Load() {
		t.Fatal("the task was accepted by Trigger but never executed by Polling")
	}
	if !errors.Is(err, errorx.ErrEngineShutdown) {
		t.Fatalf("Polling() = %v, want %v", err, errorx.ErrEngineShutdown)
	}
}

// TestPollerDeliversReadEvent is the only test that drives a real descriptor
// through the poller. It is what would have caught the fd-0 wakeup collision: a
// descriptor whose events are swallowed by the doChores branch never reaches its
// callback, and the level-triggered registration makes the loop spin instead.
func TestPollerDeliversReadEvent(t *testing.T) {
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatalf("pipe() error: %v", err)
	}
	rfd, wfd := fds[0], fds[1]
	defer unix.Close(rfd) //nolint:errcheck
	defer unix.Close(wfd) //nolint:errcheck

	p, err := netpoll.OpenPoller()
	if err != nil {
		t.Fatalf("OpenPoller() error: %v", err)
	}
	defer p.Close() //nolint:errcheck

	const payload = "ping"
	got := make(chan string, 1)

	pa := netpoll.PollAttachment{
		FD: rfd,
		Callback: func(fd int, event netpoll.IOEvent, flags netpoll.IOFlags) error {
			if netpoll.IsErrorEvent(event, flags) {
				return errorx.ErrEngineShutdown
			}
			if !netpoll.IsReadEvent(event) {
				return nil
			}
			buf := make([]byte, 16)
			n, err := unix.Read(fd, buf)
			if err != nil {
				return errorx.ErrEngineShutdown
			}
			got <- string(buf[:n])
			return errorx.ErrEngineShutdown
		},
	}

	if err := p.AddRead(&pa, false); err != nil {
		t.Fatalf("AddRead() error: %v", err)
	}

	errc := runPoller(t, p, &pa)
	if _, err := unix.Write(wfd, []byte(payload)); err != nil {
		t.Fatalf("write() error: %v", err)
	}

	perr := waitPoller(t, errc)
	if !errors.Is(perr, errorx.ErrEngineShutdown) {
		t.Fatalf("Polling() = %v, want %v", perr, errorx.ErrEngineShutdown)
	}
	select {
	case s := <-got:
		if s != payload {
			t.Fatalf("callback read %q, want %q", s, payload)
		}
	default:
		t.Fatal("the callback was never invoked for the readable descriptor")
	}
}

// TestPollerCloseIsIdempotentAndRefusesTrigger covers the descriptor-reuse hazard:
// Close used to leave the wakeup fd and the poller fd in their fields, so a second
// Close closed two descriptors that the process had since handed to something else,
// and a Trigger after Close wrote eight bytes into whatever now owned that number.
func TestPollerCloseIsIdempotentAndRefusesTrigger(t *testing.T) {
	p, err := netpoll.OpenPoller()
	if err != nil {
		t.Fatalf("OpenPoller() error: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("first Close() error: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close() error: %v, want nil (Close must be idempotent)", err)
	}

	err = p.Trigger(queue.LowPriority, func(any) error { return nil }, nil)
	if !errors.Is(err, errorx.ErrPollerClosed) {
		t.Fatalf("Trigger() after Close = %v, want %v", err, errorx.ErrPollerClosed)
	}
}
