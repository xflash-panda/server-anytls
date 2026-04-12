//nolint:all
package session

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
)

// deadlineTrackingConn wraps a net.Conn and records whether a write deadline
// was active when Write() was called.
type deadlineTrackingConn struct {
	net.Conn
	mu                 sync.Mutex
	currentDeadline    time.Time
	writesWithDeadline atomic.Int32
	totalWrites        atomic.Int32
	writeDelay         time.Duration // artificial delay inside Write to widen race window
}

func (c *deadlineTrackingConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.currentDeadline = t
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *deadlineTrackingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	hasDeadline := !c.currentDeadline.IsZero()
	c.mu.Unlock()
	c.totalWrites.Add(1)
	if hasDeadline {
		c.writesWithDeadline.Add(1)
	}
	if c.writeDelay > 0 {
		time.Sleep(c.writeDelay)
	}
	return c.Conn.Write(b)
}

func newTestServerSession(conn net.Conn) *Session {
	return NewServerSession(conn, nil, &padding.DefaultPaddingFactory)
}

// TestWriteControlFrameDeadlineLeaksToDataFrame proves that the deadline set by
// writeControlFrame at line 383 (outside connLock) can leak to concurrent
// writeDataFrame calls. This is a race condition.
//
// The test uses a slow Write() to widen the lock-hold window: while one
// goroutine holds connLock for a data frame write, another goroutine's
// writeControlFrame calls SetWriteDeadline, contaminating the connection state.
func TestWriteControlFrameDeadlineLeaksToDataFrame(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	tracker := &deadlineTrackingConn{
		Conn:       serverConn,
		writeDelay: 500 * time.Microsecond, // slow writes to widen race window
	}
	sess := newTestServerSession(tracker)
	defer sess.Close()

	// Drain client side
	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	sess.streamLock.Lock()
	sess.streams[1] = newStream(1, sess)
	sess.streamLock.Unlock()

	// Concurrently write control frames (which set deadlines) and data frames
	// (which should NOT have deadlines).
	var wg sync.WaitGroup
	const iterations = 50

	for range iterations {
		wg.Add(2)
		go func() {
			defer wg.Done()
			sess.writeControlFrame(newFrame(cmdHeartResponse, 0))
		}()
		go func() {
			defer wg.Done()
			sess.writeDataFrame(1, []byte("data"))
		}()
	}
	wg.Wait()

	totalWithDeadline := tracker.writesWithDeadline.Load()

	// Each control frame write should have exactly 1 Write() with deadline.
	// Data frame writes should have 0 writes with deadline.
	// If totalWithDeadline > iterations, deadlines leaked to data frame writes.
	expectedMax := int32(iterations) // at most 'iterations' control frame writes

	if totalWithDeadline > expectedMax {
		t.Fatalf("BUG: deadline leaked from control frames to data frames: "+
			"%d writes had deadlines (expected at most %d from control frames only)",
			totalWithDeadline, expectedMax)
	}
}

// TestWriteDataFrameNoDeadline verifies data frame writes never set deadlines.
func TestWriteDataFrameNoDeadline(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	tracker := &deadlineTrackingConn{Conn: serverConn}
	sess := newTestServerSession(tracker)
	defer sess.Close()

	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	sess.streamLock.Lock()
	sess.streams[1] = newStream(1, sess)
	sess.streamLock.Unlock()

	_, err := sess.writeDataFrame(1, []byte("hello"))
	if err != nil {
		t.Fatalf("writeDataFrame error: %v", err)
	}

	if tracker.writesWithDeadline.Load() > 0 {
		t.Fatal("writeDataFrame should never set write deadlines")
	}
}
