//nolint:errcheck
package server

import (
	"net"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/service"
)

// TestCountedConnDirectPointerTraffic verifies that CountedConn uses a direct
// userService pointer for traffic tracking, not context.Value on every I/O.
func TestCountedConnDirectPointerTraffic(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())

	cc := &CountedConn{
		Conn:        server,
		userId:      1,
		userService: us,
	}

	// Write from client side, read from CountedConn
	payload := []byte("hello")
	go func() { client.Write(payload) }()

	buf := make([]byte, 64)
	n, err := cc.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	// Verify upload traffic recorded via direct pointer
	item := us.GetTrafficItem(1)
	if item.Up.Value() != uint64(n) {
		t.Fatalf("expected Up=%d, got %d", n, item.Up.Value())
	}

	// Write from CountedConn to client
	go func() {
		b := make([]byte, 64)
		client.Read(b)
	}()

	n, err = cc.Write(payload)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Verify download traffic recorded
	if item.Down.Value() != uint64(n) {
		t.Fatalf("expected Down=%d, got %d", n, item.Down.Value())
	}
}
