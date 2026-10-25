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

//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package io

import "golang.org/x/sys/unix"

// ReadNB is read(2) for a descriptor that is in non-blocking mode.
//
// On Linux it bypasses the scheduler's syscall bracket; the BSDs (macOS above all)
// route system calls through libc wrappers that Go does not allow to be invoked
// raw, so here it is a plain unix.Read.
func ReadNB(fd int, p []byte) (int, error) { return unix.Read(fd, p) }

// WriteNB is write(2) for a descriptor that is in non-blocking mode, see ReadNB.
func WriteNB(fd int, p []byte) (int, error) { return unix.Write(fd, p) }
