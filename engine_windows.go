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
	"strings"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
)

type engine struct {
	listeners     []*listener
	opts          *Options     // options with engine
	eventLoops    loadBalancer // event-loops for handling events
	inShutdown    atomic.Bool  // whether the engine is in shutdown
	beingShutdown atomic.Bool  // whether the engine is being shutdown
	turnOff       context.CancelFunc
	eventHandler  EventHandler // user eventHandler
	concurrency   struct {
		*errgroup.Group

		ctx context.Context
	}
}

func (eng *engine) isShutdown() bool {
	return eng.inShutdown.Load() || eng.beingShutdown.Load()
}

// shutdown signals the engine to shut down.
func (eng *engine) shutdown(err error) {
	if err != nil && !errors.Is(err, errorx.ErrEngineShutdown) {
		eng.opts.Logger.Errorf("engine is being shutdown with error: %v", err)
	}
	eng.beingShutdown.Store(true)
	eng.turnOff()
}

func (eng *engine) closeEventLoops() {
	eng.shutdown(nil)
	for _, ln := range eng.listeners {
		ln.close()
	}
}

func (eng *engine) start(ctx context.Context, numEventLoop int) error {
	var el0 *eventloop
	for i := 0; i < numEventLoop; i++ {
		el := eventloop{
			ch:           make(chan any, 1024),
			eng:          eng,
			connections:  make(map[*conn]struct{}),
			eventHandler: eng.eventHandler,
		}
		eng.eventLoops.register(&el)
		eng.concurrency.Go(el.run)
		if i == 0 && eng.opts.Ticker {
			el0 = &el
		}
	}

	if el0 != nil {
		eng.concurrency.Go(func() error {
			el0.ticker(ctx)
			return nil
		})
	}

	for _, ln := range eng.listeners {
		l := ln
		if l.pc != nil {
			eng.concurrency.Go(func() error {
				return eng.ListenUDP(l.pc)
			})
		} else {
			eng.concurrency.Go(func() error {
				return eng.listenStream(l.ln)
			})
		}
	}

	return nil
}

func (eng *engine) stop(ctx context.Context, engine Engine) {
	<-ctx.Done()

	eng.eventHandler.OnShutdown(engine)

	eng.closeEventLoops()

	if err := eng.concurrency.Wait(); err != nil && !errors.Is(err, errorx.ErrEngineShutdown) {
		eng.opts.Logger.Errorf("engine shutdown error: %v", err)
	}

	eng.inShutdown.Store(true)
}

func run(eventHandler EventHandler, listeners []*listener, options *Options, addrs []string) error {
	numEventLoop := determineEventLoops(options)
	logging.Infof("Launching gnet with %d event-loops, listening on: %s",
		numEventLoop, strings.Join(addrs, " | "))

	rootCtx, shutdown := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(rootCtx)
	eng := engine{
		opts:         options,
		listeners:    listeners,
		turnOff:      shutdown,
		eventHandler: eventHandler,
		concurrency: struct {
			*errgroup.Group
			ctx context.Context
		}{eg, ctx},
	}

	switch options.LB {
	case RoundRobin:
		eng.eventLoops = new(roundRobinLoadBalancer)
		// With more than one listener, keep least-connections instead of round-robin:
		// round-robin is now concurrency-safe (nextIndex is atomic, see FIX P-1 in
		// load_balancer.go), but its counter is shared by all acceptors, so each
		// listener would hand out a different loop for the same position and a busy
		// listener could pile connections onto a loop that is already behind.
		if len(listeners) > 1 {
			eng.eventLoops = new(leastConnectionsLoadBalancer)
		}
	case LeastConnections:
		eng.eventLoops = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		eng.eventLoops = new(sourceAddrHashLoadBalancer)
	}

	engine := Engine{eng: &eng}
	switch eventHandler.OnBoot(engine) {
	case None, Close:
	case Shutdown:
		return nil
	}

	if err := eng.start(ctx, numEventLoop); err != nil {
		eng.opts.Logger.Errorf("gnet engine is stopping with error: %v", err)
		return err
	}
	defer eng.stop(rootCtx, engine)

	for _, addr := range addrs {
		// FIX S-4: see the same change in engine_unix.go — an unconditional Store
		// hid the fact that an engine already registered on this address had just
		// become unreachable to gnet.Stop, and therefore leaked.
		if prev, loaded := allEngines.LoadOrStore(addr, &eng); loaded {
			eng.opts.Logger.Warnf("gnet: an engine is already registered on %s (%p); "+
				"it is replaced in the registry and can no longer be stopped with gnet.Stop, "+
				"use Engine.Stop or a distinct address", addr, prev)
			allEngines.Store(addr, &eng)
		}
	}

	return nil
}

/*
func (eng *engine) sendCmd(_ *asyncCmd, _ bool) error {
	return errorx.ErrUnsupportedOp
}
*/
