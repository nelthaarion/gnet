//go:build 386 || arm || armbe || mips || mipsle || mips64p32 || mips64p32le || wasm

package math

import "testing"

// FIX L-6: 32-bit coverage that did not exist before. maxintHeadBit is 1<<30 here,
// so CeilToPowerOfTwo panics — rather than silently wrapping — for any n above it,
// and the largest representable power of two is 1<<30. Both boundaries are asserted
// so the guard in CeilToPowerOfTwo cannot be removed without a test noticing.
func TestCeilToPowerOfTwo32BitBounds(t *testing.T) {
	if got := CeilToPowerOfTwo(1 << 30); got != 1<<30 {
		t.Errorf("CeilToPowerOfTwo(1<<30) = %v, want %v", got, 1<<30)
	}

	// Anything strictly above maxintHeadBit (1<<30) must panic instead of returning
	// the wrapped, negative 1<<31.
	for _, n := range []int{1<<30 + 1, 1<<31 - 1} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("CeilToPowerOfTwo(%d) did not panic, want panic", n)
				}
			}()
			_ = CeilToPowerOfTwo(n)
		}()
	}
}
