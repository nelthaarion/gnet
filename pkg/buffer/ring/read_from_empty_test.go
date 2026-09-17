package ring

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type emptyResultReader struct {
	err error
}

func (r emptyResultReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestNewEmpty(t *testing.T) {
	readErr := errors.New("read failed")
	for _, state := range []string{"new", "reset", "drained"} {
		t.Run(state, func(t *testing.T) {
			for _, terminalErr := range []error{io.EOF, readErr} {
				t.Run(terminalErr.Error(), func(t *testing.T) {
					rb := New(1024)
					if state != "new" {
						if _, err := rb.Write([]byte("previous contents")); err != nil {
							t.Fatal(err)
						}
						if state == "reset" {
							rb.Reset()
						} else {
							if _, err := rb.Read(make([]byte, rb.Buffered())); err != nil {
								t.Fatal(err)
							}
						}
					}

					n, err := rb.ReadFrom(emptyResultReader{err: terminalErr})
					wantErr := terminalErr
					if terminalErr == io.EOF {
						wantErr = nil
					}
					if n != 0 || !errors.Is(err, wantErr) {
						t.Fatalf("ReadFrom = (%d, %v), want (0, %v)", n, err, wantErr)
					}
					if !rb.IsEmpty() || rb.IsFull() || rb.Buffered() != 0 {
						t.Fatalf("empty read changed buffer state: empty=%v full=%v buffered=%d",
							rb.IsEmpty(), rb.IsFull(), rb.Buffered())
					}
					if rb.Available() != rb.Cap() || len(rb.Bytes()) != 0 {
						t.Fatal("empty read made old contents available")
					}

					want := []byte("new contents")
					n, err = rb.ReadFrom(bytes.NewReader(want))
					if err != nil || n != int64(len(want)) || !bytes.Equal(rb.Bytes(), want) {
						t.Fatalf("subsequent ReadFrom = (%d, %v), contents %q", n, err, rb.Bytes())
					}
				})
			}
		})
	}
}