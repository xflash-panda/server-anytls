//nolint:errcheck
package pipe

import (
	"sync"
	"testing"
	"time"
)

// TestPipeWriteDoesNotBlockWhenReaderIsSlow proves that writing to a pipe
// should not block indefinitely when the reader is slow. This simulates
// the recvLoop head-of-line blocking issue: if one stream's reader is slow,
// the recvLoop (which writes to all pipes sequentially) gets stuck on that
// pipe and cannot deliver data to other streams.
func TestPipeWriteDoesNotBlockWhenReaderIsSlow(t *testing.T) {
	r, w := Pipe()
	defer r.Close()
	defer w.Close()

	data := []byte("hello")

	done := make(chan struct{})
	go func() {
		_, _ = w.Write(data)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("pipe write blocked when reader is not consuming - causes head-of-line blocking in recvLoop")
	}
}

// TestPipeMultipleWritesWithSlowReader proves that multiple writes can
// proceed without each one requiring immediate reader consumption.
func TestPipeMultipleWritesWithSlowReader(t *testing.T) {
	r, w := Pipe()
	defer r.Close()
	defer w.Close()

	const numWrites = 5
	data := []byte("frame-data")

	done := make(chan struct{})
	go func() {
		for i := range numWrites {
			_ = i
			_, err := w.Write(data)
			if err != nil {
				return
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("pipe blocked after multiple writes without reader - recvLoop would stall")
	}

	buf := make([]byte, 1024)
	totalRead := 0
	for totalRead < numWrites*len(data) {
		n, err := r.Read(buf)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		totalRead += n
	}

	if totalRead != numWrites*len(data) {
		t.Fatalf("expected %d bytes, got %d", numWrites*len(data), totalRead)
	}
}

// TestPipeWriteDoesNotBlockOtherPipes simulates the real recvLoop scenario:
// a single goroutine writes to multiple pipes sequentially. If one pipe's
// reader is slow, writes to other pipes should still proceed.
func TestPipeWriteDoesNotBlockOtherPipes(t *testing.T) {
	r1, w1 := Pipe()
	r2, w2 := Pipe()
	defer r1.Close()
	defer w1.Close()
	defer r2.Close()
	defer w2.Close()

	data := []byte("test-data")

	var pipe2Read sync.WaitGroup
	pipe2Read.Add(1)
	go func() {
		defer pipe2Read.Done()
		buf := make([]byte, 1024)
		_, _ = r2.Read(buf)
	}()

	done := make(chan struct{})
	go func() {
		_, _ = w1.Write(data)
		_, _ = w2.Write(data)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("writing to pipe2 blocked because pipe1's reader is slow - head-of-line blocking confirmed")
	}

	pipe2Read.Wait()
}

// TestPipeBasicReadWrite ensures the pipe still works correctly for
// normal read/write operations after any refactoring.
func TestPipeBasicReadWrite(t *testing.T) {
	r, w := Pipe()
	defer r.Close()

	expected := []byte("hello world")

	go func() {
		_, _ = w.Write(expected)
		_ = w.Close()
	}()

	buf := make([]byte, 64)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(buf[:n]) != string(expected) {
		t.Fatalf("expected %q, got %q", expected, buf[:n])
	}
}

// TestPipeCloseSignalsReader ensures closing the writer signals EOF to reader.
func TestPipeCloseSignalsReader(t *testing.T) {
	r, w := Pipe()

	go func() {
		_, _ = w.Write([]byte("data"))
		_ = w.Close()
	}()

	buf := make([]byte, 64)
	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("first read error: %v", err)
	}
	if n == 0 {
		t.Fatal("expected data on first read")
	}

	_, err = r.Read(buf)
	if err == nil {
		t.Fatal("expected error after writer closed, got nil")
	}
}
