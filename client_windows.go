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
	"net"

	"golang.org/x/sync/errgroup"

	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
)

type Client struct {
	opts *Options
	eng  *engine
}

func NewClient(eh EventHandler, opts ...Option) (cli *Client, err error) {
	options := loadOptions(opts...)
	cli = &Client{opts: options}

	logger, logFlusher := logging.GetDefaultLogger(), logging.GetDefaultFlusher()
	if options.Logger == nil {
		if options.LogPath != "" {
			// FIX L-8: see createListeners in gnet.go — the error was discarded, and
			// installing the (nil, nil) pair it returns on failure made every later
			// logging call panic on a nil receiver.
			fileLogger, fileFlusher, logErr := logging.CreateLoggerAsLocalFile(options.LogPath, options.LogLevel)
			if logErr != nil {
				logging.Errorf("failed to create a logger on %s, keeping the current logger: %v",
					options.LogPath, logErr)
			} else {
				logger, logFlusher = fileLogger, fileFlusher
			}
		}
		options.Logger = logger
	} else {
		logger = options.Logger
		logFlusher = nil
	}
	logging.SetDefaultLoggerAndFlusher(logger, logFlusher)

	rootCtx, shutdown := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(rootCtx)
	eng := engine{
		listeners:    []*listener{},
		opts:         options,
		turnOff:      shutdown,
		eventHandler: eh,
		eventLoops:   newLoadBalancerForClient(options.LB),
		concurrency: struct {
			*errgroup.Group
			ctx context.Context
		}{eg, ctx},
	}
	cli.eng = &eng
	return
}

func (cli *Client) Start() error {
	numEventLoop := determineEventLoops(cli.opts)
	logging.Infof("Starting gnet client with %d event loops", numEventLoop)

	cli.eng.eventHandler.OnBoot(Engine{cli.eng})

	var el0 *eventloop
	for i := 0; i < numEventLoop; i++ {
		el := eventloop{
			ch:           make(chan any, 1024),
			eng:          cli.eng,
			connections:  make(map[*conn]struct{}),
			eventHandler: cli.eng.eventHandler,
		}
		cli.eng.eventLoops.register(&el)
		cli.eng.concurrency.Go(el.run)
		if cli.opts.Ticker && el.idx == 0 {
			el0 = &el
		}
	}

	if el0 != nil {
		ctx := cli.eng.concurrency.ctx
		cli.eng.concurrency.Go(func() error {
			el0.ticker(ctx)
			return nil
		})
	}

	logging.Debugf("default logging level is %s", logging.LogLevel())

	return nil
}

func (cli *Client) Stop() error {
	cli.eng.shutdown(nil)

	cli.eng.eventHandler.OnShutdown(Engine{cli.eng})

	// Notify all event-loops to exit.
	cli.eng.closeEventLoops()

	// Wait for all event-loops to exit.
	err := cli.eng.concurrency.Wait()

	// Put the engine into the shutdown state.
	cli.eng.inShutdown.Store(true)

	// Flush the logger.
	logging.Cleanup()

	return err
}

func (cli *Client) Dial(network, addr string) (Conn, error) {
	return cli.DialContext(network, addr, nil)
}

func (cli *Client) DialContext(network, addr string, ctx any) (Conn, error) {
	var (
		c   net.Conn
		err error
	)
	c, err = net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return cli.EnrollContext(c, ctx)
}

func (cli *Client) Enroll(nc net.Conn) (gc Conn, err error) {
	return cli.EnrollContext(nc, nil)
}

func (cli *Client) EnrollContext(nc net.Conn, ctx any) (gc Conn, err error) {
	if nc == nil {
		return nil, errorx.ErrInvalidNetConn
	}
	if cli.eng.isShutdown() {
		return nil, errorx.ErrEngineInShutdown
	}
	el := cli.eng.eventLoops.next(nil)
	connOpened := make(chan error, 1)
	publish := func(c *conn, udp bool) error {
		if err := el.enqueue(&openConn{c: c, cb: func(err error) { connOpened <- err }}); err != nil {
			_ = nc.Close()
			c.release()
			return err
		}
		if err := <-connOpened; err != nil {
			return err
		}
		if err := el.readLoop(nc, c, ctx, udp); err != nil {
			_ = el.enqueue(&netErr{c, err})
			return err
		}
		return nil
	}
	switch v := nc.(type) {
	case *net.TCPConn:
		if cli.opts.TCPNoDelay == TCPNoDelay {
			if err = v.SetNoDelay(true); err != nil {
				return
			}
		}
		c := newStreamConn(el, nc, ctx)
		if opts := cli.opts; opts.TCPKeepAlive > 0 {
			idle := opts.TCPKeepAlive
			intvl := opts.TCPKeepInterval
			if intvl == 0 {
				intvl = opts.TCPKeepAlive / 5
			}
			cnt := opts.TCPKeepCount
			if opts.TCPKeepCount == 0 {
				cnt = 5
			}
			if err = c.SetKeepAlive(true, idle, intvl, cnt); err != nil {
				return
			}
		}
		if err = publish(c, false); err != nil {
			return nil, err
		}
		gc = c
	case *net.UnixConn:
		c := newStreamConn(el, nc, ctx)
		if err = publish(c, false); err != nil {
			return nil, err
		}
		gc = c
	case *net.UDPConn:
		c := newUDPConn(el, nil, nc, nc.LocalAddr(), nc.RemoteAddr(), ctx)
		if err = publish(c, true); err != nil {
			return nil, err
		}
		gc = c
	default:
		return nil, errorx.ErrUnsupportedProtocol
	}
	return
}
