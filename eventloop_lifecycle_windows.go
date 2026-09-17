package gnet

import (
	"errors"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	bbPool "github.com/nelthaarion/gnet/v2/pkg/pool/bytebuffer"
)

// ErrEventQueueFull means a nonblocking event submission was not accepted.
// The caller retains ownership and may retry after making progress.
var ErrEventQueueFull = errors.New("gnet: event queue is full")

// enqueue is for I/O producers, never for the event-loop goroutine itself.
// Cancellation unblocks producers before teardown takes the exclusive lock.
func (el *eventloop) enqueue(v any) error {
	el.submitMu.RLock()
	defer el.submitMu.RUnlock()
	if el.stopped || el.eng.beingShutdown.Load() {
		return errorx.ErrEngineInShutdown
	}
	select {
	case <-el.eng.concurrency.ctx.Done():
		return errorx.ErrEngineInShutdown
	case el.ch <- v:
		return nil
	}
}

// submit never waits for the loop to receive. In particular, callbacks may use
// it without deadlocking their own loop when its queue is full.
func (el *eventloop) submit(fn func() error) error {
	el.submitMu.RLock()
	defer el.submitMu.RUnlock()
	if el.stopped || el.eng.beingShutdown.Load() {
		return errorx.ErrEngineInShutdown
	}
	select {
	case el.ch <- fn:
		return nil
	default:
		return ErrEventQueueFull
	}
}

// finishPending runs only on the event-loop goroutine, after admission stops.
// Closures complete against closed connections; pending opens are rejected.
func (el *eventloop) finishPending() {
	for {
		select {
		case item := <-el.ch:
			switch v := item.(type) {
			case *openConn:
				_ = v.c.rawConn.Close()
				v.c.release()
				if v.cb != nil {
					v.cb(errorx.ErrEngineInShutdown)
				}
			case *tcpConn:
				bbPool.Put(v.b)
			case *udpConn:
				v.c.release()
			case func() error:
				_ = v()
			}
		default:
			return
		}
	}
}
