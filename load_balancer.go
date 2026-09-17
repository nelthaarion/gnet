// Copyright (c) 2019 The Gnet Authors. All rights reserved.
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

package gnet

import (
	"hash/crc32"
	"net"
	"sync/atomic"

	"github.com/nelthaarion/gnet/v2/pkg/bs"
	"github.com/nelthaarion/gnet/v2/pkg/logging"
)

// LoadBalancing represents the type of load-balancing algorithm.
type LoadBalancing int

const (
	// RoundRobin assigns the next accepted connection to the event-loop by polling event-loop list.
	RoundRobin LoadBalancing = iota

	// LeastConnections assigns the next accepted connection to the event-loop that is
	// serving the least number of active connections at the current time.
	LeastConnections

	// SourceAddrHash assigns the next accepted connection to the event-loop by hashing the remote address.
	SourceAddrHash
)

type (
	// loadBalancer is an interface which manipulates the event-loop set.
	loadBalancer interface {
		register(*eventloop)
		next(net.Addr) *eventloop
		index(int) *eventloop
		iterate(func(int, *eventloop) bool)
		len() int
	}

	// baseLoadBalancer with base lb.
	baseLoadBalancer struct {
		eventLoops []*eventloop
		size       int
	}

	// roundRobinLoadBalancer with Round-Robin algorithm.
	//
	// FIX P-1: nextIndex was read and written without synchronisation, causing a
	// data race when Engine.Register() calls next() concurrently with the acceptor
	// goroutine on a multicore engine. Changed to atomic.Uint64 so every increment
	// and load is a single indivisible operation without a mutex.
	roundRobinLoadBalancer struct {
		baseLoadBalancer
		nextIndex atomic.Uint64
	}

	// leastConnectionsLoadBalancer with Least-Connections algorithm.
	leastConnectionsLoadBalancer struct {
		baseLoadBalancer
	}

	// sourceAddrHashLoadBalancer with Hash algorithm.
	sourceAddrHashLoadBalancer struct {
		baseLoadBalancer
	}
)

// ==================================== Implementation of base load-balancer ====================================

// register adds a new eventloop into load-balancer.
func (lb *baseLoadBalancer) register(el *eventloop) {
	el.idx = lb.size
	lb.eventLoops = append(lb.eventLoops, el)
	lb.size++
}

// index returns the eligible eventloop by index.
func (lb *baseLoadBalancer) index(i int) *eventloop {
	if i >= lb.size {
		return nil
	}
	return lb.eventLoops[i]
}

// iterate iterates all the eventloops.
func (lb *baseLoadBalancer) iterate(f func(int, *eventloop) bool) {
	for i, el := range lb.eventLoops {
		if !f(i, el) {
			break
		}
	}
}

// len returns the length of event-loop list.
func (lb *baseLoadBalancer) len() int {
	return lb.size
}

// ==================================== Implementation of Round-Robin load-balancer ====================================

// next returns the eligible event-loop based on Round-Robin algorithm.
//
// FIX P-1: Use atomic.Uint64.Add so concurrent callers from the acceptor goroutine
// and Engine.Register() goroutines never race on nextIndex. The previous plain
// lb.nextIndex++ was an unsynchronised read-modify-write on a shared integer.
func (lb *roundRobinLoadBalancer) next(_ net.Addr) *eventloop {
	// Guard against an engine that has no event-loop registered yet: the modulo
	// below is a division by zero in that case.
	if lb.size == 0 {
		return nil
	}
	// Add(1) returns the new value; subtract 1 to get the pre-increment index.
	idx := lb.nextIndex.Add(1) - 1
	return lb.eventLoops[idx%uint64(lb.size)]
}

// ================================= Implementation of Least-Connections load-balancer =================================

// next returns the event-loop that is currently serving the fewest connections.
//
// The scan is O(number of event-loops) on every call and deliberately unsynchronised:
// countConn reads an atomic counter per loop, so the worst a concurrent acceptor can
// do is pick a loop whose count changed a moment later — every loop stays within one
// connection of the true minimum, which is all least-connections needs to guarantee.
// The event-loop count is bounded by Options.Multicore, i.e. by the number of CPUs,
// so the scan is short and not worth a heap or a lock to avoid.
func (lb *leastConnectionsLoadBalancer) next(_ net.Addr) (el *eventloop) {
	if lb.size == 0 {
		return nil
	}
	el = lb.eventLoops[0]
	minN := el.countConn()
	for _, v := range lb.eventLoops[1:] {
		if n := v.countConn(); n < minN {
			minN = n
			el = v
		}
	}
	return
}

// ==================================== Selection of the load-balancer ====================================

// newLoadBalancerForClient selects the load-balancer of a Client from Options.LB.
//
// FIX M-12: Client hardcoded leastConnectionsLoadBalancer and ignored Options.LB, so a
// caller that asked for RoundRobin silently kept paying for the O(event-loops) count
// scan on every Dial.  SourceAddrHash cannot be honoured for a client-initiated
// connection: Dial asks the balancer for an event-loop before the connection exists,
// so there is no remote address to hash (the call passes nil), and hashing the local
// ephemeral port would spread nothing.  That choice is downgraded to
// least-connections with a warning instead of being ignored in silence.
func newLoadBalancerForClient(lb LoadBalancing) loadBalancer {
	switch lb {
	case RoundRobin:
		return new(roundRobinLoadBalancer)
	case LeastConnections:
		return new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		logging.Warnf("SourceAddrHash is not applicable to a Client, falling back to LeastConnections")
		return new(leastConnectionsLoadBalancer)
	default:
		logging.Warnf("unknown load-balancing algorithm %d, falling back to LeastConnections", lb)
		return new(leastConnectionsLoadBalancer)
	}
}

// ======================================= Implementation of Hash load-balancer ========================================

// hash converts a string to a hash code.
//
// FIX L-4: this returned an int computed as `v := int(crc32.ChecksumIEEE(...)); if
// v < 0 { return -v }`. crc32 yields a uint32, so on a 32-bit platform the
// conversion made half of all addresses negative and the negation of 0x80000000
// (math.MinInt32) overflowed back to itself — a negative hash code, and from there a
// negative slice index in next(). Keeping the value unsigned makes the result
// identical on every word size.
func (*sourceAddrHashLoadBalancer) hash(s string) uint32 {
	return crc32.ChecksumIEEE(bs.StringToBytes(s))
}

// next returns the eligible event-loop by taking the remainder of a hash code as the index of event-loop list.
func (lb *sourceAddrHashLoadBalancer) next(netAddr net.Addr) *eventloop {
	if lb.size == 0 {
		return nil
	}
	// FIX L-4: netAddr is nil whenever the caller has no remote address to hash —
	// Engine.Register reaches here with the address from the connection or the
	// context, and neither is guaranteed to be set. Reading it unconditionally
	// panicked inside netAddr.String(). A nil address hashes as the empty string,
	// so such connections all land on the same loop instead of crashing.
	var s string
	if netAddr != nil {
		s = netAddr.String()
	}
	return lb.eventLoops[uint64(lb.hash(s))%uint64(lb.size)]
}
