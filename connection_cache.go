package gnet

import bsPool "github.com/nelthaarion/gnet/v2/pkg/pool/byteslice"

// Peek results must not be recycled by another Peek or Next call.
func (c *conn) releasePeekCaches() {
	for i, buf := range c.peekCaches {
		bsPool.Put(buf)
		c.peekCaches[i] = nil
	}
	c.peekCaches = c.peekCaches[:0]
}
