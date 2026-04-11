//nolint:all
package session

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
)

// TestSessionRunNilPadding verifies that Session.Run does not panic
// when the padding atomic pointer has not been initialized (Load returns nil).
func TestSessionRunNilPadding(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create an uninitialized atomic pointer — Load() returns nil
	var uninitPadding atomic.Pointer[padding.PaddingFactory]

	sess := NewClientSession(serverConn, &uninitPadding)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Session.Run panicked with nil padding: %v", r)
		}
	}()

	// Drain so writes don't block
	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	sess.Run()
}

// TestWriteConnLockedNilPadding verifies that writeConnLocked does not panic
// when the padding pointer is nil.
func TestWriteConnLockedNilPadding(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	var uninitPadding atomic.Pointer[padding.PaddingFactory]

	sess := NewClientSession(serverConn, &uninitPadding)
	sess.sendPadding = true
	sess.pktCounter.Store(0)

	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("writeConnLocked panicked with nil padding: %v", r)
		}
	}()

	sess.connLock.Lock()
	_, _ = sess.writeConnLocked([]byte("test"))
	sess.connLock.Unlock()
}
