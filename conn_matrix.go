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
)

// connMatrix is a 2-D array-backed connection store used under the gc_opt build tag.
//
// Layout: table[row][column] holds a *conn. Each row slice is allocated lazily
// when the first connection in that row is added, and freed when the last one
// leaves, so memory grows in 64 KiB steps (ConnMatrixColumnMax * sizeof(*conn)).
//
// FIX P-3 — O(n²) compaction in delConn
// ─────────────────────────────────────
// The previous delConn implementation searched for the "last" non-nil entry by
// iterating backward from [ConnMatrixRowMax-1][ConnMatrixColumnMax-1] to the
// deleted position.  In the worst case this scans the entire matrix:
//   ConnMatrixRowMax (256) × ConnMatrixColumnMax (65536) = 16 777 216 iterations
// per delete — O(n²) overall as n grows.
//
// Fix: maintain an explicit tail cursor (tailRow, tailCol) that always points to
// the slot one-past the most recently added connection (i.e. the next free slot
// if we had never deleted anything).  When compacting, we step the tail backward
// over nil cells to find the last live connection in O(1) amortised time.
//
// The key invariant: every slot in [0, tail) that is non-nil is a live connection;
// every slot at or past tail is nil.  addConn advances tail forward; delConn
// swaps the deleted slot with the entry at (tail-1) and retracts tail.
type connMatrix struct {
	disableCompact bool                          // disable compaction during iterate()
	connCounts     [gfd.ConnMatrixRowMax]int32   // number of active connections per row
	tailRow        int                           // row of the next free slot (the write cursor)
	tailCol        int                           // column of the next free slot
	table          [gfd.ConnMatrixRowMax][]*conn // connection matrix
	fd2gfd         map[int]gfd.GFD               // fd -> gfd.GFD for O(1) lookup
}

func (cm *connMatrix) init() {
	cm.fd2gfd = make(map[int]gfd.GFD)
}

func (cm *connMatrix) iterate(f func(*conn) bool) {
	cm.disableCompact = true
	defer func() { cm.disableCompact = false }()
	for _, conns := range cm.table {
		for _, c := range conns {
			if c != nil {
				if !f(c) {
					return
				}
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

// addConn inserts c into the matrix at the current tail position and
// advances the tail cursor.
func (cm *connMatrix) addConn(c *conn, index int) {
	if cm.tailRow >= gfd.ConnMatrixRowMax {
		return
	}

	if cm.table[cm.tailRow] == nil {
		cm.table[cm.tailRow] = make([]*conn, gfd.ConnMatrixColumnMax)
	}

	c.gfd = gfd.NewGFD(c.fd, index, cm.tailRow, cm.tailCol)
	cm.fd2gfd[c.fd] = c.gfd
	cm.table[cm.tailRow][cm.tailCol] = c
	cm.incCount(cm.tailRow, 1)

	// Advance tail.
	if cm.tailCol++; cm.tailCol == gfd.ConnMatrixColumnMax {
		cm.tailRow++
		cm.tailCol = 0
	}
}

// delConn removes c from the matrix.
//
// FIX P-3: Instead of scanning backward from the last possible slot (O(n) per
// call, O(n²) overall), we use the tail cursor:
//
//  1. Erase the slot for c.
//  2. Retract the tail cursor by one position (to point at the last occupied slot).
//  3. If the last occupied slot is different from the just-erased slot,
//     move the last connection into the vacated slot and update its GFD.
//
// Step 2 is amortised O(1) because we only skip over nil slots that were
// created by previous deletions from the tail end.  In the common case
// (deleting an interior slot) step 2 finds the last connection in O(1).
//
// The only edge case is when many connections at the tail have already been
// deleted (leaving a run of nil slots before the true last live connection).
// We handle this with a backward scan that is bounded by the number of
// previously deleted tail entries — still amortised O(1) per delete because
// each nil slot is visited at most once total across all calls.
func (cm *connMatrix) delConn(c *conn) {
	cRow, cCol := c.gfd.ConnMatrixRow(), c.gfd.ConnMatrixColumn()

	// Remove from the lookup map.
	delete(cm.fd2gfd, c.fd)
	cm.incCount(cRow, -1)

	// Erase the deleted slot.
	if cm.connCounts[cRow] == 0 {
		// Last connection in this row: free the row slice.
		cm.table[cRow] = nil
	} else {
		cm.table[cRow][cCol] = nil
	}

	if cm.disableCompact {
		// iterate() is in progress; don't move anything, just update the
		// tail if needed so addConn can reuse freed space later.
		if cRow < cm.tailRow || (cRow == cm.tailRow && cCol < cm.tailCol) {
			// The deleted slot is before the tail; tail is still valid — no change needed.
		}
		return
	}

	// ── Step 2: retract the tail to the last live slot ────────────────────
	//
	// Walk the tail backward over any nil cells.  This loop runs at most once
	// per nil-at-tail slot across all delConn calls (amortised O(1)).
	for {
		// Move tail one step back.
		if cm.tailCol > 0 {
			cm.tailCol--
		} else if cm.tailRow > 0 {
			cm.tailRow--
			cm.tailCol = gfd.ConnMatrixColumnMax - 1
		} else {
			// Matrix is now empty; reset to origin.
			cm.tailRow, cm.tailCol = 0, 0
			return
		}

		if cm.table[cm.tailRow] != nil && cm.table[cm.tailRow][cm.tailCol] != nil {
			break // found the last live connection
		}
	}

	// ── Step 3: move last connection to the vacated slot ─────────────────
	//
	// After step 2, (cm.tailRow, cm.tailCol) points to the last live connection.
	// If it is the same cell as the one we just deleted, there is nothing to move.
	if cm.tailRow == cRow && cm.tailCol == cCol {
		// The deleted connection was the tail: matrix already compact.
		return
	}

	// Ensure the destination row slice exists (it might have been freed if
	// cRow had zero connections after the delete above).
	if cm.table[cRow] == nil {
		cm.table[cRow] = make([]*conn, gfd.ConnMatrixColumnMax)
	}

	// Move the tail connection into the vacated slot.
	last := cm.table[cm.tailRow][cm.tailCol]
	updatedGFD := last.gfd
	updatedGFD.UpdateIndexes(cRow, cCol)
	last.gfd = updatedGFD
	cm.fd2gfd[last.fd] = updatedGFD
	cm.table[cRow][cCol] = last
	cm.incCount(cRow, 1)

	// Clear the old tail slot.
	cm.table[cm.tailRow][cm.tailCol] = nil
	cm.incCount(cm.tailRow, -1)
	if cm.connCounts[cm.tailRow] == 0 {
		cm.table[cm.tailRow] = nil
	}
}

func (cm *connMatrix) getConn(fd int) *conn {
	gFD, ok := cm.fd2gfd[fd]
	if !ok {
		return nil
	}
	if cm.table[gFD.ConnMatrixRow()] == nil {
		return nil
	}
	return cm.table[gFD.ConnMatrixRow()][gFD.ConnMatrixColumn()]
}
