// Copyright (c) 2025 The Gnet Authors. All rights reserved.
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

package elastic

import (
	"bytes"
	"testing"
)

func TestPeekStatic(t *testing.T) {
	mb, err := New(64)
	if err != nil {
		t.Fatal(err)
	}

	head, tail, complete := mb.PeekStatic()
	if len(head) != 0 || len(tail) != 0 || !complete {
		t.Fatalf("empty buffer: head=%d tail=%d complete=%v", len(head), len(tail), complete)
	}

	// ring only
	_, _ = mb.Write([]byte("hello "))
	_, _ = mb.Write([]byte("world"))
	head, tail, complete = mb.PeekStatic()
	if string(head) != "hello world" || len(tail) != 0 || !complete {
		t.Fatalf("ring only: head=%q tail=%q complete=%v", head, tail, complete)
	}
	if n, _ := mb.Discard(6); n != 6 {
		t.Fatalf("discard: %d", n)
	}
	head, _, _ = mb.PeekStatic()
	if string(head) != "world" {
		t.Fatalf("after discard: head=%q", head)
	}

	// peeking must not consume
	if mb.Buffered() != 5 {
		t.Fatalf("buffered=%d", mb.Buffered())
	}

	// overflow into the list part: complete turns false and the ring part is still peekable
	_, _ = mb.Discard(5)
	big := bytes.Repeat([]byte{'x'}, 200)
	_, _ = mb.Write(big)
	_, _ = mb.Write([]byte("tail"))
	head, tail, complete = mb.PeekStatic()
	if complete {
		t.Fatal("complete must be false while the list part holds data")
	}
	if len(head)+len(tail) == 0 || len(head)+len(tail) > mb.Buffered() {
		t.Fatalf("static part: head=%d tail=%d buffered=%d", len(head), len(tail), mb.Buffered())
	}

	// the allocation-free promise
	mb2, _ := New(1 << 16)
	_, _ = mb2.Write(make([]byte, 1000))
	if allocs := testing.AllocsPerRun(100, func() { _, _, _ = mb2.PeekStatic() }); allocs != 0 {
		t.Fatalf("PeekStatic allocates %.1f times per call", allocs)
	}
}
