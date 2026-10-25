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

//go:build linux

package io

import (
	"bytes"
	"testing"

	"golang.org/x/sys/unix"
)

func nonblockingPair(t *testing.T) (a, b int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	return fds[0], fds[1]
}

func TestReadWriteNB(t *testing.T) {
	a, b := nonblockingPair(t)

	// nothing to read yet: EAGAIN, n == -1 like unix.Read, and comparable with unix.EAGAIN
	buf := make([]byte, 16)
	n, err := ReadNB(b, buf)
	if n != -1 || err != unix.EAGAIN {
		t.Fatalf("empty socket: n=%d err=%v, want -1 / EAGAIN", n, err)
	}

	msg := []byte("raw syscalls")
	if n, err = WriteNB(a, msg); n != len(msg) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if n, err = ReadNB(b, buf); n != len(msg) || err != nil || !bytes.Equal(buf[:n], msg) {
		t.Fatalf("read: n=%d err=%v data=%q", n, err, buf[:max(n, 0)])
	}

	// zero-length operations must not touch the (nil) buffer
	if n, err = WriteNB(a, nil); n != 0 || err != nil {
		t.Fatalf("empty write: n=%d err=%v", n, err)
	}
	if n, err = ReadNB(b, nil); n != -1 && n != 0 {
		t.Fatalf("empty read: n=%d err=%v", n, err)
	}

	// peer closed: read returns 0, EOF is the caller's business
	_ = unix.Close(a)
	if n, err = ReadNB(b, buf); n != 0 || err != nil {
		t.Fatalf("after peer close: n=%d err=%v, want 0 / nil", n, err)
	}
}

func TestWriteNBErrors(t *testing.T) {
	if n, err := WriteNB(-1, []byte("x")); n != -1 || err != unix.EBADF {
		t.Fatalf("bad fd: n=%d err=%v, want -1 / EBADF", n, err)
	}
	if n, err := ReadNB(-1, make([]byte, 1)); n != -1 || err != unix.EBADF {
		t.Fatalf("bad fd: n=%d err=%v, want -1 / EBADF", n, err)
	}

	// EPIPE: write to a socket whose peer is gone (SIGPIPE is ignored by the Go runtime for
	// non-stdout descriptors, which is what the engine relies on as well)
	a, b := nonblockingPair(t)
	_ = unix.Close(b)
	if n, err := WriteNB(a, []byte("x")); n != -1 || err != unix.EPIPE {
		t.Fatalf("broken pipe: n=%d err=%v, want -1 / EPIPE", n, err)
	}
}

// A full socket buffer must report a short write or EAGAIN, never block the thread.
func TestWriteNBFullBufferDoesNotBlock(t *testing.T) {
	a, _ := nonblockingPair(t)
	chunk := make([]byte, 64*1024)
	total := 0
	for i := 0; i < 1<<14; i++ {
		n, err := WriteNB(a, chunk)
		if err == unix.EAGAIN {
			if total == 0 {
				t.Fatal("EAGAIN before any byte was accepted")
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error after %d bytes: %v", total, err)
		}
		total += n
	}
	t.Fatalf("socket buffer never filled up after %d bytes", total)
}

// The errno boxing must not allocate on the hot EAGAIN path.
func TestReadNBNoAllocOnEAGAIN(t *testing.T) {
	_, b := nonblockingPair(t)
	buf := make([]byte, 64)
	if allocs := testing.AllocsPerRun(200, func() { _, _ = ReadNB(b, buf) }); allocs != 0 {
		t.Fatalf("ReadNB allocates %.1f times per call on EAGAIN", allocs)
	}
}
