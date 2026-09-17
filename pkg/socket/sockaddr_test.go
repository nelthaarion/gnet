// Copyright (c) 2024 The Gnet Authors. All rights reserved.
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

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package socket

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	bsPool "github.com/nelthaarion/gnet/v2/pkg/pool/byteslice"
)

// poisonPool returns 32-byte blocks to the pool with their contents set to c, so
// that any byte a caller exposes from a recycled block is visible in the result.
// bsPool.Get does not clear the memory it hands back.
func poisonPool(c byte, times int) {
	for i := 0; i < times; i++ {
		buf := bsPool.Get(32)
		for j := range buf {
			buf[j] = c
		}
		bsPool.Put(buf)
	}
}

// TestItod pins the pooled-buffer conversion behind ip6ZoneToString: it must
// return the decimal digits and nothing else, whatever the pool hands back.
// Returning buf[i:] instead of buf[i+1:] prefixed every number with one stale
// byte of a previously pooled block, which then travelled into net.TCPAddr.Zone
// and net.UDPAddr.Zone.
func TestItod(t *testing.T) {
	// FIX L-6: 1<<32 is not representable as a uint on 32-bit GOARCH, so as a slice
	// literal it made this file fail to compile there. The values that fit a 32-bit
	// uint are always tested; the wider ones are appended only where uint is 64 bits.
	values := []uint{1, 7, 42, 255, 65535, ^uint(0)}
	if strconv.IntSize == 64 {
		wide := uint(1)
		values = append(values, wide<<32, wide<<40)
	}

	for _, c := range []byte{0x00, 0xAA, 0xFF, 'x'} {
		poisonPool(c, 64)
		for _, v := range values {
			require.Equalf(t, strconv.FormatUint(uint64(v), 10), itod(v),
				"itod(%d) leaked bytes from the pool (poisoned with %#x)", v, c)
		}
		require.Equal(t, "0", itod(0))
	}
}

// TestIP6ZoneToString checks the same conversion through its public caller,
// including the zone of a SockaddrInet6 that is handed back to the application.
func TestIP6ZoneToString(t *testing.T) {
	poisonPool(0xAA, 64)

	// Zone 0 means "no zone".
	require.Empty(t, ip6ZoneToString(0))

	// An index with no matching interface falls back to the decimal index.
	require.Equal(t, "4294967295", ip6ZoneToString(^uint32(0)))
	require.Equal(t, "12345", ip6ZoneToString(12345))

	// A real interface resolves to its name instead.
	lo, err := net.InterfaceByName("lo")
	if err != nil {
		lo, err = net.InterfaceByName("lo0") // macOS and the BSDs
	}
	require.NoError(t, err, "neither lo nor lo0 exists")
	require.Equal(t, lo.Name, ip6ZoneToString(uint32(lo.Index)))

	// The zone must survive the conversion to a net.Addr untouched.
	addr := SockaddrToTCPOrUnixAddr(&unix.SockaddrInet6{ZoneId: 12345})
	tcpAddr, ok := addr.(*net.TCPAddr)
	require.True(t, ok)
	require.Equal(t, "12345", tcpAddr.Zone)
}

// TestSockaddrRoundTrip checks that addresses survive a conversion to a
// Sockaddr and back, for both address families.
func TestSockaddrRoundTrip(t *testing.T) {
	for _, addr := range []net.Addr{
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080},
		&net.TCPAddr{IP: net.ParseIP("::1"), Port: 443},
		&net.UDPAddr{IP: net.ParseIP("192.168.1.1"), Port: 53},
		&net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 5353, Zone: "lo"},
	} {
		sa := NetAddrToSockaddr(addr)
		require.NotNilf(t, sa, "no sockaddr for %v", addr)

		switch a := addr.(type) {
		case *net.TCPAddr:
			got, ok := SockaddrToTCPOrUnixAddr(sa).(*net.TCPAddr)
			require.True(t, ok)
			require.Truef(t, a.IP.Equal(got.IP) && a.Port == got.Port, "got %v, want %v", got, a)
		case *net.UDPAddr:
			got, ok := SockaddrToUDPAddr(sa).(*net.UDPAddr)
			require.True(t, ok)
			require.Truef(t, a.IP.Equal(got.IP) && a.Port == got.Port, "got %v, want %v", got, a)
		}
	}

	// An unparsable address has no Sockaddr.
	require.Nil(t, NetAddrToSockaddr(&net.UnixAddr{Name: "/tmp/x", Net: "bogus"}))
}
