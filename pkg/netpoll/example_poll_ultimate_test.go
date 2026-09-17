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

//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && poll_opt

package netpoll_test

import "github.com/nelthaarion/gnet/v2/pkg/netpoll"

// examplePoll drives the poller for the poll_opt build.
//
// Under poll_opt there is no callback parameter: the PollAttachment is carried in
// the event's Udata field and restored by the poller itself, which is what
// PollAttachment.Callback is for.  Hence the attachment argument goes unused here.
func examplePoll(p *netpoll.Poller, _ *netpoll.PollAttachment) error {
	return p.Polling()
}
