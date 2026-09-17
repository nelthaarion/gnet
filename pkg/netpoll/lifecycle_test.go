//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package netpoll

import (
	"errors"
	"sync"
	"testing"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/queue"
)

func TestTriggerConcurrentClose(t *testing.T) {
	for round := 0; round < 20; round++ {
		p, err := OpenPoller()
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for j := 0; j < 20; j++ {
					err := p.Trigger(queue.HighPriority, func(any) error { return nil }, nil)
					if err != nil && !errors.Is(err, errorx.ErrPollerClosed) {
						t.Errorf("Trigger: %v", err)
					}
				}
			}()
		}
		close(start)
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if err := p.Trigger(queue.HighPriority, func(any) error { return nil }, nil); !errors.Is(err, errorx.ErrPollerClosed) {
			t.Fatalf("Trigger after close: %v", err)
		}
		// No consumer was started: release admitted tasks after all producers exit.
		discardPendingTasks(p.urgentAsyncTaskQueue, p.asyncTaskQueue)
	}
}