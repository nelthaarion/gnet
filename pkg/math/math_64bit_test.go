//go:build !386 && !arm && !armbe && !mips && !mipsle && !mips64p32 && !mips64p32le && !wasm

package math

import "testing"

// FIX L-6: these cases belong to the 64-bit-only table in TestCeilToPowerOfTwo.
// Their inputs and expected values exceed math.MaxInt32, so as untyped constants
// assigned to an int field they made the package fail to compile on every 32-bit
// GOARCH. Keeping them in a build-tagged file lets both word sizes be tested
// without either one losing coverage.
func TestCeilToPowerOfTwo64Bit(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want int
	}{
		// Just above the 32-bit ceiling, where CeilToPowerOfTwo still works here.
		{name: "huge_1G_plus_1", n: 1<<30 + 1, want: 1 << 31},

		// Around 2^32.
		{name: "extreme_2_32_minus_1", n: 1<<32 - 1, want: 1 << 32},
		{name: "extreme_2_32", n: 1 << 32, want: 1 << 32},
		{name: "extreme_2_32_plus_1", n: 1<<32 + 1, want: 1 << 33},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CeilToPowerOfTwo(tt.n); got != tt.want {
				t.Errorf("CeilToPowerOfTwo(%d) = %v, want %v", tt.n, got, tt.want)
			}
		})
	}
}
