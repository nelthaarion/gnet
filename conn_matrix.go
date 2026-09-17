// Copyright (c) 2023 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && gc_opt

package gnet

import (
	"sync/atomic"

	"github.com/nelthaarion/gnet/v2/internal/gfd"
	errorx "github.com/nelthaarion/gnet/v2/pkg/errors"
)

// connMatrix maintains a dense prefix of live connections. Deletion normally
// swaps in the final connection in O(1). During iteration moves are deferred,
// so callbacks cannot skip connections by deleting them.
type connMatrix struct {
	disableCompact bool
	connCounts     [gfd.ConnMatrixRowMax]int32
	tailRow        int
	tailCol        int
	table          [gfd.ConnMatrixRowMax][]*conn
	fd2gfd         map[int]gfd.GFD
	dirty          bool
}

func (cm *connMatrix) init() {
	cm.fd2gfd = make(map[int]gfd.GFD)
}

func (cm *connMatrix) iterate(f func(*conn) bool) {
	wasDisabled := cm.disableCompact
	cm.disableCompact = true
	defer func() {
		cm.disableCompact = wasDisabled
		if !wasDisabled && cm.dirty {
			cm.compact()
		}
	}()
	for _, conns := range cm.table {
		for _, c := range conns {
			if c != nil && !f(c) {
				return
			}
		}
	}
}

func (cm *connMatrix) incCount(row int, delta int32) {
	atomic.AddInt32(&cm.connCounts[row], delta)
}

func (cm *connMatrix) loadCount() (n int32) {
	for i := range cm.connCounts {
		n += atomic.LoadInt32(&cm.connCounts[i])
	}
	return
}

func (cm *connMatrix) addConn(c *conn, index int) error {
	if cm.tailRow >= gfd.ConnMatrixRowMax {
		return errorx.ErrConnMatrixFull
	}
	if cm.table[cm.tailRow] == nil {
		cm.table[cm.tailRow] = make([]*conn, gfd.ConnMatrixColumnMax)
	}
	c.gfd = gfd.NewGFD(c.fd, index, cm.tailRow, cm.tailCol)
	cm.fd2gfd[c.fd] = c.gfd
	cm.table[cm.tailRow][cm.tailCol] = c
	cm.incCount(cm.tailRow, 1)
	cm.setTail(cm.tailRow*gfd.ConnMatrixColumnMax + cm.tailCol + 1)
	return nil
}

func (cm *connMatrix) setTail(n int) {
	cm.tailRow, cm.tailCol = n/gfd.ConnMatrixColumnMax, n%gfd.ConnMatrixColumnMax
}

// move transfers a live connection into an empty earlier slot.
func (cm *connMatrix) move(c *conn, row, col int) {
	oldRow, oldCol := c.gfd.ConnMatrixRow(), c.gfd.ConnMatrixColumn()
	if cm.table[row] == nil {
		cm.table[row] = make([]*conn, gfd.ConnMatrixColumnMax)
	}
	cm.table[row][col] = c
	cm.table[oldRow][oldCol] = nil
	cm.incCount(row, 1)
	cm.incCount(oldRow, -1)
	if cm.connCounts[oldRow] == 0 {
		cm.table[oldRow] = nil
	}
	c.gfd.UpdateIndexes(row, col)
	cm.fd2gfd[c.fd] = c.gfd
}

func (cm *connMatrix) delConn(c *conn) {
	row, col := c.gfd.ConnMatrixRow(), c.gfd.ConnMatrixColumn()
	delete(cm.fd2gfd, c.fd)
	cm.table[row][col] = nil
	cm.incCount(row, -1)
	if cm.connCounts[row] == 0 {
		cm.table[row] = nil
	}
	if cm.disableCompact {
		cm.dirty = true
		return
	}

	// The tail slot itself was deleted: do not move an earlier survivor forward.
	last := cm.tailRow*gfd.ConnMatrixColumnMax + cm.tailCol - 1
	cm.setTail(last)
	if last == row*gfd.ConnMatrixColumnMax+col {
		return
	}
	cm.move(cm.table[cm.tailRow][cm.tailCol], row, col)
}

// compact restores the dense prefix once the outermost iteration completes.
// Each live slot is visited once; ordinary deletion never scans the matrix.
func (cm *connMatrix) compact() {
	next := 0
	for row := range cm.table {
		for col := 0; cm.table[row] != nil && col < gfd.ConnMatrixColumnMax; col++ {
			c := cm.table[row][col]
			if c == nil {
				continue
			}
			destRow, destCol := next/gfd.ConnMatrixColumnMax, next%gfd.ConnMatrixColumnMax
			if row != destRow || col != destCol {
				cm.move(c, destRow, destCol)
			}
			next++
		}
	}
	cm.setTail(next)
	cm.dirty = false
}

func (cm *connMatrix) getConn(fd int) *conn {
	gFD, ok := cm.fd2gfd[fd]
	if !ok || cm.table[gFD.ConnMatrixRow()] == nil {
		return nil
	}
	return cm.table[gFD.ConnMatrixRow()][gFD.ConnMatrixColumn()]
}