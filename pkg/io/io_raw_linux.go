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
	"unsafe"

	"golang.org/x/sys/unix"
)

// Do the interface allocations only once for the Errno values the event-loop
// compares against on every call.
var (
	errEAGAIN error = unix.EAGAIN
	errEINTR  error = unix.EINTR
	errEINVAL error = unix.EINVAL
)

// errnoErr returns common boxed Errno values, to prevent allocations at runtime.
// The returned values are comparable with the unix.Errno constants, i.e.
// `err == unix.EAGAIN` keeps working.
func errnoErr(e unix.Errno) error {
	switch e {
	case unix.EAGAIN:
		return errEAGAIN
	case unix.EINTR:
		return errEINTR
	case unix.EINVAL:
		return errEINVAL
	}
	return e
}

var _zero uintptr

// ReadNB is read(2) for a descriptor that is in non-blocking mode.
//
// unix.Read goes through syscall.Syscall, which brackets every call with
// runtime.entersyscall/exitsyscall so that the scheduler can take the P away from
// a goroutine that blocks in the kernel. For a non-blocking descriptor that
// protection buys nothing: the call returns within microseconds either with data
// or with EAGAIN, and the bracket is a measurable share of the per-request cost
// (two atomic status transitions plus the P hand-off checks, on every read).
// ReadNB issues the same system call through RawSyscall instead.
//
// It MUST only be used with descriptors that have O_NONBLOCK set (every socket
// that gnet accepts or dials does); on a blocking descriptor it would block the
// OS thread together with its P and stall the garbage collector.
//
// Like unix.Read it returns n == -1 together with the error when the call fails.
func ReadNB(fd int, p []byte) (n int, err error) {
	var ptr unsafe.Pointer
	if len(p) > 0 {
		ptr = unsafe.Pointer(&p[0])
	} else {
		ptr = unsafe.Pointer(&_zero)
	}
	r, _, e := unix.RawSyscall(unix.SYS_READ, uintptr(fd), uintptr(ptr), uintptr(len(p))) //nolint:staticcheck
	if e != 0 {
		return -1, errnoErr(e)
	}
	return int(r), nil
}

// WriteNB is write(2) for a descriptor that is in non-blocking mode, see ReadNB for
// the reasoning and for the requirement that the descriptor must be non-blocking.
//
// Like unix.Write it returns n == -1 together with the error when the call fails.
func WriteNB(fd int, p []byte) (n int, err error) {
	var ptr unsafe.Pointer
	if len(p) > 0 {
		ptr = unsafe.Pointer(&p[0])
	} else {
		ptr = unsafe.Pointer(&_zero)
	}
	r, _, e := unix.RawSyscall(unix.SYS_WRITE, uintptr(fd), uintptr(ptr), uintptr(len(p))) //nolint:staticcheck
	if e != 0 {
		return -1, errnoErr(e)
	}
	return int(r), nil
}
