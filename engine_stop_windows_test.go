package gnet

import (
	"net"
	"testing"
)

// Exercise the existing two-engine shutdown scenario without relying on port
// 9998 being available on the development machine.
func TestWindowsEngineStopIsolatedPort(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	// Release the reservation so the existing helper can bind both listeners.
	// Another process could claim it in this interval; this is not an atomic
	// reservation, but avoids the known persistent fixed-port listeners.
	t.Logf("running existing engine-stop scenario on %s", addr)
	testEngineStop(t, "tcp4", addr)
}
