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

//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && !poll_opt

package netpoll_test

import "github.com/nelthaarion/gnet/v2/pkg/netpoll"

// examplePoll drives the poller for the default build.
//
// FIX: the example used to call Polling with a callback unconditionally, so the
// whole netpoll example — and with it the netpoll test binary — failed to compile
// under the poll_opt tag, leaving that build untested by `go vet`/`go test`.  The
// call now lives behind this build-tagged wrapper, next to the poll_opt variant in
// example_poll_ultimate_test.go.
func examplePoll(p *netpoll.Poller, pa *netpoll.PollAttachment) error {
	return p.Polling(func(fd int, event netpoll.IOEvent, flags netpoll.IOFlags) error {
		return pa.Callback(fd, event, flags)
	})
}
