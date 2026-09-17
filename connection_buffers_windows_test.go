package gnet

import (
	"bytes"
	"errors"
	"io"
	"testing"

	bbPool "github.com/nelthaarion/gnet/v2/pkg/pool/bytebuffer"
)

type suffixTestWriter struct {
	calls int
	err   error
}

func (w *suffixTestWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return len(p), nil
	}
	return 1, w.err
}

func TestWindowsWriteToRetainsSuffix(t *testing.T) {
	for _, terminal := range []error{nil, io.ErrClosedPipe} {
		c := &conn{buffer: bbPool.Get()}
		_, _ = c.inboundBuffer.Write([]byte("abc"))
		_, _ = c.buffer.Write([]byte("def"))
		n, err := c.WriteTo(&suffixTestWriter{err: terminal})
		wantErr := terminal
		if wantErr == nil {
			wantErr = io.ErrShortWrite
		}
		if n != 4 || !errors.Is(err, wantErr) || string(c.buffer.B) != "ef" {
			t.Fatalf("WriteTo=(%d,%v), suffix=%q", n, err, c.buffer.B)
		}
		var dst bytes.Buffer
		n, err = c.WriteTo(&dst)
		if n != 2 || err != nil || dst.String() != "ef" {
			t.Fatalf("retry=(%d,%v), data=%q", n, err, dst.String())
		}
		c.release()
	}
}

func TestWindowsPeekOwnsAllCopies(t *testing.T) {
	c := &conn{buffer: bbPool.Get()}
	_, _ = c.inboundBuffer.Write([]byte("abc"))
	_, _ = c.buffer.Write([]byte("def"))
	first, err := c.Peek(4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Peek(6)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "abcd" || string(second) != "abcdef" || len(c.peekCaches) != 2 {
		t.Fatal("Peek copies were lost or overwritten")
	}
	_, _ = c.Next(2)
	if string(first) != "abcd" || string(second) != "abcdef" {
		t.Fatal("Next invalidated a Peek copy")
	}
	_, _ = c.Discard(-1)
	if len(c.peekCaches) != 0 {
		t.Fatal("Discard retained Peek allocations")
	}
	c.release()
}
