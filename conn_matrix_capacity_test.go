//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && gc_opt

package gnet

import (
	"testing"

	"github.com/nelthaarion/gnet/v2/internal/gfd"
)

func TestConnMatrixReclaimsCapacity(t *testing.T) {
	var cm connMatrix
	cm.init()
	survivor := &conn{fd: 1}
	if err := cm.addConn(survivor, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*gfd.ConnMatrixColumnMax; i++ {
		c := &conn{fd: 2}
		if err := cm.addConn(c, 0); err != nil {
			t.Fatal(err)
		}
		cm.delConn(c)
		if cm.tailRow != 0 || cm.tailCol != 1 || cm.getConn(1) != survivor {
			t.Fatalf("iteration %d: capacity not reclaimed: cursor=(%d,%d)", i, cm.tailRow, cm.tailCol)
		}
	}
	cm.delConn(survivor)
	if cm.tailRow != 0 || cm.tailCol != 0 || cm.loadCount() != 0 {
		t.Fatal("empty matrix did not reset")
	}
}

func TestConnMatrixReclaimsIterationHoles(t *testing.T) {
	var cm connMatrix
	cm.init()
	live := make(map[int]*conn)
	for i := 1; i <= gfd.ConnMatrixColumnMax+3; i++ {
		c := &conn{fd: i}
		if err := cm.addConn(c, 0); err != nil {
			t.Fatal(err)
		}
		live[i] = c
	}
	cm.iterate(func(c *conn) bool {
		if c.fd%2 == 0 {
			cm.delConn(c)
			delete(live, c.fd)
		}
		return true
	})
	if msg := checkConnMatrixInvariants(&cm, live); msg != "" {
		t.Fatal(msg)
	}
	if got := cm.tailRow*gfd.ConnMatrixColumnMax + cm.tailCol; got != len(live) {
		t.Fatalf("cursor %d, live count %d", got, len(live))
	}
	cm.iterate(func(c *conn) bool {
		cm.delConn(c)
		return true
	})
	if cm.tailRow != 0 || cm.tailCol != 0 || cm.loadCount() != 0 {
		t.Fatal("iteration did not reclaim all slots")
	}
}