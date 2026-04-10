//nolint:errcheck
package pipe

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// newTestPipe creates a pipe with a small maxBuf for testing edge cases.
func newTestPipe(maxBuf int) (*PipeReader, *PipeWriter) {
	p := &pipe{
		done:          make(chan struct{}),
		notify:        make(chan struct{}),
		readDeadline:  MakePipeDeadline(),
		writeDeadline: MakePipeDeadline(),
		maxBuf:        maxBuf,
	}
	r := &PipeReader{p}
	w := &PipeWriter{r: r}
	return r, w
}

// --- Original head-of-line blocking tests ---

func TestPipeWriteDoesNotBlockWhenReaderIsSlow(t *testing.T) {
	r, w := Pipe()
	defer r.Close()
	defer w.Close()

	done := make(chan struct{})
	go func() {
		_, _ = w.Write([]byte("hello"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("pipe write blocked when reader is not consuming")
	}
}

func TestPipeMultipleWritesWithSlowReader(t *testing.T) {
	r, w := Pipe()
	defer r.Close()
	defer w.Close()

	const numWrites = 5
	data := []byte("frame-data")

	done := make(chan struct{})
	go func() {
		for range numWrites {
			if _, err := w.Write(data); err != nil {
				return
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("pipe blocked after multiple writes without reader")
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
		t.Fatal("writing to pipe2 blocked because pipe1's reader is slow")
	}
	pipe2Read.Wait()
}

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

// --- Buffer boundary and compaction tests ---

// TestPipeWriteExactlyMaxBuf verifies writing exactly maxBuf bytes works
// and that subsequent writes block until reader consumes.
func TestPipeWriteExactlyMaxBuf(t *testing.T) {
	const maxBuf = 64
	r, w := newTestPipe(maxBuf)
	defer r.Close()
	defer w.Close()

	data := make([]byte, maxBuf)
	for i := range data {
		data[i] = byte(i)
	}

	// Write exactly maxBuf bytes — should succeed without blocking
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if n != maxBuf {
		t.Fatalf("expected to write %d bytes, wrote %d", maxBuf, n)
	}

	// Next write should block because buffer is full
	blocked := make(chan struct{})
	go func() {
		_, _ = w.Write([]byte{0xFF})
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("write should block when buffer is full")
	case <-time.After(50 * time.Millisecond):
		// Expected: write is blocked
	}

	// Read all data, unblocking the writer
	buf := make([]byte, maxBuf)
	n, err = r.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(buf[:n], data[:n]) {
		t.Fatal("data mismatch")
	}

	// Writer should now be unblocked
	select {
	case <-blocked:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("writer still blocked after reader consumed data")
	}
}

// TestPipeCompactionPreservesData verifies that buffer compaction
// (triggered when off > maxBuf/2) preserves data integrity.
func TestPipeCompactionPreservesData(t *testing.T) {
	const maxBuf = 64
	r, w := newTestPipe(maxBuf)
	defer r.Close()
	defer w.Close()

	// Fill buffer to maxBuf
	fillData := bytes.Repeat([]byte("A"), maxBuf)
	_, err := w.Write(fillData)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Read more than half to advance offset past maxBuf/2
	// This should trigger compaction on next consumeLocked call
	readBuf := make([]byte, maxBuf/2+1)
	n, err := r.Read(readBuf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if n != maxBuf/2+1 {
		t.Fatalf("expected to read %d bytes, got %d", maxBuf/2+1, n)
	}

	// Write more data — this should succeed since we freed space
	newData := []byte("BCDE")
	_, err = w.Write(newData)
	if err != nil {
		t.Fatalf("second write error: %v", err)
	}

	// Read remaining data — should be the un-read A's + BCDE
	var allRead []byte
	buf := make([]byte, 128)
	for {
		n, err = r.Read(buf)
		allRead = append(allRead, buf[:n]...)
		if err != nil {
			break
		}
		if len(allRead) >= (maxBuf-maxBuf/2-1)+len(newData) {
			break
		}
	}

	remaining := maxBuf - (maxBuf/2 + 1)
	expected := append(bytes.Repeat([]byte("A"), remaining), newData...)
	if !bytes.Equal(allRead, expected) {
		t.Fatalf("data mismatch after compaction:\nexpected: %q\ngot:      %q", expected, allRead)
	}
}

// TestPipeWriteBlocksWhenFull verifies write blocks when buffer is full,
// and unblocks after partial read frees space.
func TestPipeWriteBlocksWhenFull(t *testing.T) {
	const maxBuf = 32
	r, w := newTestPipe(maxBuf)
	defer r.Close()
	defer w.Close()

	// Fill the buffer completely
	_, err := w.Write(make([]byte, maxBuf))
	if err != nil {
		t.Fatalf("fill error: %v", err)
	}

	// Subsequent write should block
	writeDone := make(chan int)
	go func() {
		n, _ := w.Write([]byte("extra"))
		writeDone <- n
	}()

	select {
	case <-writeDone:
		t.Fatal("write should block on full buffer")
	case <-time.After(50 * time.Millisecond):
	}

	// Partial read to free space
	buf := make([]byte, 8)
	_, err = r.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	// Writer should unblock
	select {
	case n := <-writeDone:
		if n != 5 {
			t.Fatalf("expected to write 5 bytes, wrote %d", n)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("writer still blocked after partial read")
	}
}

// --- Concurrent safety tests ---

// TestPipeConcurrentReadWrite runs concurrent readers and writers
// with race detector to verify no data races.
func TestPipeConcurrentReadWrite(t *testing.T) {
	const maxBuf = 128
	r, w := newTestPipe(maxBuf)

	const totalBytes = 10000
	writeData := make([]byte, totalBytes)
	rand.Read(writeData)

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		offset := 0
		for offset < totalBytes {
			chunk := min(37, totalBytes-offset) // prime-sized chunks
			n, err := w.Write(writeData[offset : offset+chunk])
			if err != nil {
				return
			}
			offset += n
		}
		w.Close()
	}()

	// Reader goroutine
	var readData []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 43) // different prime-sized buffer
		for {
			n, err := r.Read(buf)
			if n > 0 {
				readData = append(readData, buf[:n]...)
			}
			if err != nil {
				return
			}
		}
	}()

	wg.Wait()

	if !bytes.Equal(writeData, readData) {
		t.Fatalf("data corruption: wrote %d bytes, read %d bytes", len(writeData), len(readData))
	}
}

// TestPipeCloseWriterWhileReaderBlocked verifies reader gets EOF
// when writer closes while reader is blocked waiting for data.
func TestPipeCloseWriterWhileReaderBlocked(t *testing.T) {
	r, w := newTestPipe(64)

	readDone := make(chan error)
	go func() {
		buf := make([]byte, 64)
		_, err := r.Read(buf)
		readDone <- err
	}()

	// Give reader time to block
	time.Sleep(20 * time.Millisecond)
	w.Close()

	select {
	case err := <-readDone:
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reader still blocked after writer closed")
	}
}

// TestPipeCloseReaderWhileWriterBlocked verifies writer gets ErrClosedPipe
// when reader closes while writer is blocked on full buffer.
func TestPipeCloseReaderWhileWriterBlocked(t *testing.T) {
	const maxBuf = 32
	r, w := newTestPipe(maxBuf)

	// Fill buffer
	_, err := w.Write(make([]byte, maxBuf))
	if err != nil {
		t.Fatalf("fill error: %v", err)
	}

	writeDone := make(chan error)
	go func() {
		_, err := w.Write([]byte("blocked"))
		writeDone <- err
	}()

	// Give writer time to block
	time.Sleep(20 * time.Millisecond)
	r.Close()

	select {
	case err := <-writeDone:
		if err != io.ErrClosedPipe {
			t.Fatalf("expected io.ErrClosedPipe, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("writer still blocked after reader closed")
	}
}

// TestPipeWriteAfterClose verifies writing after close returns error.
func TestPipeWriteAfterClose(t *testing.T) {
	_, w := newTestPipe(64)
	w.Close()
	_, err := w.Write([]byte("data"))
	if err == nil {
		t.Fatal("expected error writing to closed pipe")
	}
}

// TestPipeReadAfterClose verifies reading from closed pipe returns error.
func TestPipeReadAfterClose(t *testing.T) {
	r, _ := newTestPipe(64)
	r.Close()
	buf := make([]byte, 64)
	_, err := r.Read(buf)
	if err == nil {
		t.Fatal("expected error reading from closed pipe")
	}
}

// TestPipeDrainBufferAfterWriterClose verifies all buffered data
// can be read after writer closes.
func TestPipeDrainBufferAfterWriterClose(t *testing.T) {
	r, w := newTestPipe(256)

	expected := []byte("all this data must survive writer close")
	_, err := w.Write(expected)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	w.Close()

	// Should be able to read all data even after close
	var got []byte
	buf := make([]byte, 10) // small reads to test multiple drains
	for {
		n, err := r.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if err != nil {
			break
		}
	}

	if !bytes.Equal(got, expected) {
		t.Fatalf("data lost after writer close:\nexpected: %q\ngot:      %q", expected, got)
	}
}

// --- Deadline tests ---

// TestPipeReadDeadline verifies that an expired read deadline causes Read to return.
func TestPipeReadDeadline(t *testing.T) {
	r, w := newTestPipe(64)
	defer w.Close()

	_ = r.SetReadDeadline(time.Now().Add(20 * time.Millisecond))

	buf := make([]byte, 64)
	_, err := r.Read(buf)
	if err != os.ErrDeadlineExceeded {
		t.Fatalf("expected os.ErrDeadlineExceeded, got %v", err)
	}
}

// TestPipeWriteDeadline verifies that an expired write deadline causes Write to return.
func TestPipeWriteDeadline(t *testing.T) {
	const maxBuf = 32
	r, w := newTestPipe(maxBuf)
	defer r.Close()

	// Fill buffer so next write blocks
	_, err := w.Write(make([]byte, maxBuf))
	if err != nil {
		t.Fatalf("fill error: %v", err)
	}

	_ = w.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))

	_, err = w.Write([]byte("blocked"))
	if err != os.ErrDeadlineExceeded {
		t.Fatalf("expected os.ErrDeadlineExceeded, got %v", err)
	}
}

// TestPipeResetDeadline verifies that resetting deadline to zero allows operations again.
func TestPipeResetDeadline(t *testing.T) {
	r, w := newTestPipe(64)
	defer w.Close()

	// Set expired deadline
	_ = r.SetReadDeadline(time.Now().Add(-1 * time.Second))
	buf := make([]byte, 64)
	_, err := r.Read(buf)
	if err != os.ErrDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	// Reset deadline
	_ = r.SetReadDeadline(time.Time{})

	// Write data and verify read works
	go func() {
		_, _ = w.Write([]byte("after-reset"))
	}()

	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("read after reset error: %v", err)
	}
	if string(buf[:n]) != "after-reset" {
		t.Fatalf("expected 'after-reset', got %q", buf[:n])
	}
}

// TestPipeLargeDataIntegrity writes and reads a large amount of data
// through a small-buffered pipe to stress compaction and verify integrity.
func TestPipeLargeDataIntegrity(t *testing.T) {
	const maxBuf = 100
	r, w := newTestPipe(maxBuf)

	const totalBytes = 50000
	writeData := make([]byte, totalBytes)
	rand.Read(writeData)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = w.Write(writeData)
		w.Close()
	}()

	var readData []byte
	buf := make([]byte, 17) // odd size to stress partial reads
	for {
		n, err := r.Read(buf)
		if n > 0 {
			readData = append(readData, buf[:n]...)
		}
		if err != nil {
			break
		}
	}

	wg.Wait()

	if !bytes.Equal(writeData, readData) {
		t.Fatalf("data integrity failed: wrote %d bytes, read %d bytes", totalBytes, len(readData))
	}
}
