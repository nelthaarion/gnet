package main

import "testing"

func TestParseHead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		head      string
		wantLen   int
		wantClose bool
	}{
		{"plain GET", "GET /bench HTTP/1.1\r\nHost: x\r\n\r\n", 0, false},
		{"keep-alive explicit", "GET / HTTP/1.1\r\nConnection: keep-alive\r\n\r\n", 0, false},
		{"close", "GET / HTTP/1.1\r\nConnection: close\r\n\r\n", 0, true},
		{"close, odd case", "GET / HTTP/1.1\r\nconnection: Close\r\n\r\n", 0, true},
		{"close among tokens", "GET / HTTP/1.1\r\nConnection: TE, close\r\n\r\n", 0, true},
		{"content-length", "POST / HTTP/1.1\r\nContent-Length: 12\r\n\r\n", 12, false},
		{"content-length lower case, no space", "POST / HTTP/1.1\r\ncontent-length:345\r\n\r\n", 345, false},
		{"content-length and close", "POST / HTTP/1.1\r\nHost: x\r\nCONTENT-LENGTH:  7 \r\nConnection: close\r\n\r\n", 7, true},
		{"header value that merely mentions close", "GET / HTTP/1.1\r\nX-Note: please close this\r\n\r\n", 0, false},
		{"bare LF line endings", "GET / HTTP/1.1\nContent-Length: 3\nConnection: close\n\n", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, c := parseHead([]byte(tc.head))
			if n != tc.wantLen || c != tc.wantClose {
				t.Fatalf("parseHead = (%d, %v), want (%d, %v)", n, c, tc.wantLen, tc.wantClose)
			}
		})
	}
}

func TestParseHeadDoesNotAllocate(t *testing.T) {
	head := []byte("GET /bench HTTP/1.1\r\nHost: localhost:8080\r\nUser-Agent: wrk\r\nAccept: */*\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	if allocs := testing.AllocsPerRun(200, func() { parseHead(head) }); allocs != 0 {
		t.Fatalf("parseHead allocates %.1f times per request", allocs)
	}
}
