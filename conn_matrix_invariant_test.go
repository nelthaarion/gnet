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

//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && gc_opt

package gnet

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/nelthaarion/gnet/v2/internal/gfd"
)

// TestConnMatrixDelLastConnIsNotOverwritten pins down the regression that the
// tail cursor of connMatrix used to cause: deleting the connection that sat in
// the highest occupied slot rewinded the cursor onto a *lower* slot, and the
// connection that was moved up into the vacated slot ended up at or above the
// cursor.  The following two additions then overwrote it, so its fd resolved to
// a different connection and the connection itself leaked (never closed, no
// OnClose, one fd and one buffer set per leak).
//
// The three operations below are the smallest sequence that triggers it, so the
// test needs no goroutines, no sockets and no timing to be reliable.
func TestConnMatrixDelLastConnIsNotOverwritten(t *testing.T) {
	cm := &connMatrix{}
	cm.init()
	el := eventloop{engine: &engine{opts: &Options{}}}

	newConn := func(fd int) *conn {
		return newStreamConn("tcp", fd, &el, &unix.SockaddrInet4{}, &net.TCPAddr{}, &net.TCPAddr{})
	}

	a, b := newConn(11), newConn(12)
	require.NoError(t, cm.addConn(a, 0))
	require.NoError(t, cm.addConn(b, 0))
	require.Equal(t, 0, a.gfd.ConnMatrixRow())
	require.Equal(t, 0, a.gfd.ConnMatrixColumn())
	require.Equal(t, 1, b.gfd.ConnMatrixColumn())

	// b is the highest occupied slot: deleting it must leave a in place and make
	// b's slot the next free one, so the capacity is reclaimed immediately.
	cm.delConn(b)
	require.Equal(t, a, cm.getConn(a.fd), "a must stay reachable after the deletion")
	require.Equal(t, 0, a.gfd.ConnMatrixRow())
	require.Equal(t, 0, a.gfd.ConnMatrixColumn(), "a is expected to stay in its slot")

	// These two additions used to land on a's slot.
	c, d := newConn(13), newConn(14)
	require.NoError(t, cm.addConn(c, 0))
	require.NoError(t, cm.addConn(d, 0))

	require.Equal(t, a, cm.getConn(a.fd), "a has been overwritten by another connection")
	require.Equal(t, c, cm.getConn(c.fd), "c has been overwritten by another connection")
	require.Equal(t, d, cm.getConn(d.fd), "d has been overwritten by another connection")
	require.Equal(t, 3, int(cm.loadCount()), "the matrix holds exactly a, c and d")
}

// TestConnMatrixInvariants churns a large number of randomized additions and
// deletions through a real connMatrix and verifies after every single step that
// the structure is still self-consistent.  It is the broad net under the
// targeted regression above: any future change to the cursor handling that
// breaks the layout is caught here rather than as a cross-connection bug in
// production.
func TestConnMatrixInvariants(t *testing.T) {
	const steps = 20000

	rnd := rand.New(rand.NewSource(0xC0FFEE))
	cm := &connMatrix{}
	cm.init()
	el := eventloop{engine: &engine{opts: &Options{}}}
	live := make(map[int]*conn)
	var free []*conn
	nextFD := 100

	for i := 0; i < steps; i++ {
		// Two thirds of the steps add a connection, the rest delete a random one.
		if len(live) == 0 || (rnd.Intn(3) != 0 && len(live) < 1000) {
			if len(free) == 0 {
				free = append(free, newStreamConn("tcp", nextFD, &el, &unix.SockaddrInet4{}, &net.TCPAddr{}, &net.TCPAddr{}))
				nextFD++
			}
			c := free[len(free)-1]
			free = free[:len(free)-1]
			require.NoError(t, cm.addConn(c, 0))
			live[c.fd] = c
		} else {
			n := rnd.Intn(len(live))
			var victim *conn
			for _, c := range live {
				if n == 0 {
					victim = c
					break
				}
				n--
			}
			delete(live, victim.fd)
			cm.delConn(victim)
			free = append(free, victim)
		}

		if msg := checkConnMatrixInvariants(cm, live); msg != "" {
			t.Fatalf("step %d: %s", i, msg)
		}
	}

	t.Logf("connMatrix kept every invariant across %d randomized operations", steps)
}

// checkConnMatrixInvariants returns a description of the first broken invariant,
// or an empty string when the matrix is consistent with live.
func checkConnMatrixInvariants(cm *connMatrix, live map[int]*conn) string {
	var problems []string

	// The table holds each live connection exactly once, at the slot it believes
	// it occupies.
	inTable := make(map[*conn]struct{}, len(live))
	for i, row := range cm.table {
		for j, c := range row {
			if c == nil {
				continue
			}
			if _, ok := live[c.fd]; !ok {
				problems = append(problems, fmt.Sprintf("slot (%d,%d) holds fd=%d which is not live", i, j, c.fd))
			} else if live[c.fd] != c {
				problems = append(problems, fmt.Sprintf("slot (%d,%d) holds fd=%d but a different conn object is live under that fd", i, j, c.fd))
			}
			if c.gfd.ConnMatrixRow() != i || c.gfd.ConnMatrixColumn() != j {
				problems = append(problems, fmt.Sprintf("fd=%d is at slot (%d,%d) but its gfd says (%d,%d)",
					c.fd, i, j, c.gfd.ConnMatrixRow(), c.gfd.ConnMatrixColumn()))
			}
			if c.gfd.Fd() != c.fd {
				problems = append(problems, fmt.Sprintf("fd=%d has a gfd for fd=%d", c.fd, c.gfd.Fd()))
			}
			if _, dup := inTable[c]; dup {
				problems = append(problems, fmt.Sprintf("fd=%d appears in more than one slot", c.fd))
			}
			inTable[c] = struct{}{}

			// Every live connection must sit strictly below the cursor, otherwise
			// the next addConn writes over it.
			if i > cm.tailRow || (i == cm.tailRow && j >= cm.tailCol) {
				problems = append(problems, fmt.Sprintf("fd=%d at slot (%d,%d) is at or past the cursor (%d,%d)",
					c.fd, i, j, cm.tailRow, cm.tailCol))
			}
		}
	}
	if len(inTable) != len(live) {
		problems = append(problems, fmt.Sprintf("the table holds %d connections, %d are live", len(inTable), len(live)))
	}

	// The cursor points at a free slot, or one past the end of the matrix.
	if cm.tailRow < gfd.ConnMatrixRowMax && cm.table[cm.tailRow] != nil {
		if c := cm.table[cm.tailRow][cm.tailCol]; c != nil {
			problems = append(problems, fmt.Sprintf("the cursor (%d,%d) points at live fd=%d", cm.tailRow, cm.tailCol, c.fd))
		}
	}

	// Row counters and the fd lookup agree with the table.
	for i, row := range cm.table {
		want := int32(0)
		for _, c := range row {
			if c != nil {
				want++
			}
		}
		if got := cm.connCounts[i]; got != want {
			problems = append(problems, fmt.Sprintf("row %d counts %d connections but holds %d", i, got, want))
		}
	}
	for fd, c := range live {
		if got := cm.getConn(fd); got != c {
			if got == nil {
				problems = append(problems, fmt.Sprintf("fd=%d is live but getConn cannot find it", fd))
			} else {
				problems = append(problems, fmt.Sprintf("getConn(%d) returned fd=%d instead", fd, got.fd))
			}
		}
	}
	if got, want := int(cm.loadCount()), len(live); got != want {
		problems = append(problems, fmt.Sprintf("loadCount is %d but %d connections are live", got, want))
	}

	return strings.Join(problems, "; ")
}
