// Copyright (c) 2020 The Gnet Authors. All rights reserved.
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

// Package bs provides a few handy bytes/string functions.
//
// FIX L-11: both functions below are zero-copy aliases, which makes them easy to
// misuse and hard to debug when misused — the failure is a silently corrupted
// string or slice somewhere else in the program, not a panic here. The contract
// for each is spelled out on the function. If you cannot satisfy it, copy: an
// ordinary []byte(s) or string(b) is correct in every case and usually cheap
// enough. The one existing caller in this repository (the source-address hash
// load balancer) only reads the result, which is the safe direction.
package bs

import (
	"unsafe"
)

// BytesToString converts byte slice to a string without any memory allocation.
//
// The returned string aliases b's backing array: the string is not a copy and
// does not own its bytes. Two consequences, both the caller's responsibility:
//
//   - Any later write to b — or to any other slice sharing that array, which
//     includes a buffer that gets recycled through a pool — is visible in the
//     string. Go strings are immutable by contract, so this breaks assumptions
//     the runtime and the rest of the program may rely on.
//   - b must stay alive as long as the string does. It does here, because the
//     string keeps a pointer to the array, but that also means the backing array
//     cannot be collected while a short substring of it is retained.
//
// Only use this when b is never written again and never returned to a pool. When
// in doubt, use string(b).
func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// StringToBytes converts string to a byte slice without any memory allocation.
//
// The returned slice aliases the string's backing array, which the runtime places
// in read-only memory for string literals. Writing through the result is
// therefore undefined behaviour — it can fault, and for a string that is not a
// literal it can silently corrupt every other string sharing that array.
//
// The returned slice must never be mutated, never handed to a pool or a cache
// that will later reuse it, and never returned to a caller that expects to own
// it. This is exactly how the FIX S-2 defect in connection_unix.go arose: a slice
// produced here was passed to bsPool.Put, so the pool began handing out memory
// owned by a live string. When in doubt, use []byte(s).
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
