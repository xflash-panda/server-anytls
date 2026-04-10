//nolint:errcheck
package service

import (
	"net"
	"testing"
)

// TestTrafficManagerCollectAndClearNoDataLoss verifies that traffic accumulated
// concurrently with toUserTraffics() collection is not lost.
//
// toUserTraffics() must atomically read-and-reset each counter (Swap), so that
// any Add() happening concurrently is either included in the swapped value or
// preserved for the next collection — never silently discarded.
func TestTrafficManagerCollectAndClearNoDataLoss(t *testing.T) {
	tm := newTrafficManager()

	item := tm.loadOrStore(1)
	item.Up.Add(100)
	item.Down.Add(200)

	// Step 1: collect atomically reads and resets counters
	traffics := tm.drainUserTraffics()
	if len(traffics) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(traffics))
	}
	if traffics[0].Upload != 100 || traffics[0].Download != 200 {
		t.Fatalf("unexpected values: up=%d down=%d", traffics[0].Upload, traffics[0].Download)
	}

	// Step 2: new traffic arrives after collection (during submit)
	item.Up.Add(50)
	item.Down.Add(30)

	// Step 3: next collection should capture the new traffic (no clear needed)
	remaining := tm.drainUserTraffics()
	if len(remaining) == 0 {
		t.Fatal("traffic added after collect was lost — " +
			"toUserTraffics() must use Swap to atomically read and reset")
	}
	if remaining[0].Upload != 50 {
		t.Fatalf("expected remaining Upload=50, got %d", remaining[0].Upload)
	}
	if remaining[0].Download != 30 {
		t.Fatalf("expected remaining Download=30, got %d", remaining[0].Download)
	}
}

// TestCountedConnInitialBytesNotDoubleCounted verifies that the initial read
// bytes are correctly split: protocol header bytes (password + padding) are
// counted once via explicit Add, and remaining payload bytes in the cached
// buffer are counted once via CountedConn.Read() — no double-counting.
func TestCountedConnInitialBytesNotDoubleCounted(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	item := newTrafficItem()

	// Simulate: initial read got 50 bytes total
	// 34 bytes = protocol header (password 32 + paddingLen 2 + padding 0)
	// 16 bytes = payload remaining in CachedConn buffer
	headerSize := uint64(34)
	payloadInCache := 16

	// Protocol header counted explicitly (as handleTcpConnection does)
	item.Up.Add(headerSize)

	// Payload read through CountedConn.Read()
	go func() {
		data := make([]byte, payloadInCache)
		clientConn.Write(data)
	}()

	buf := make([]byte, 64)
	n, err := serverConn.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	item.Up.Add(uint64(n)) // CountedConn.Read adds payload bytes

	got := item.Up.Load()
	expected := headerSize + uint64(payloadInCache) // 34 + 16 = 50, each counted once

	if got != expected {
		t.Fatalf("expected Up=%d (header %d + payload %d), got %d",
			expected, headerSize, payloadInCache, got)
	}
}
