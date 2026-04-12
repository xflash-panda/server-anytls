package pipe

import (
	"io"
	"os"
	"sync"
	"time"
)

type onceError struct {
	sync.Mutex
	err error
}

func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}

func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}

const defaultBufSize = 256 * 1024 // 256KB per stream buffer

type pipe struct {
	mu sync.Mutex

	buf []byte
	off int // read offset into buf

	once sync.Once
	done chan struct{}

	// notify is a signal channel: closed when data is available or space freed.
	// After consuming the signal, it must be replaced with a fresh channel.
	notify chan struct{}

	rerr onceError
	werr onceError

	readDeadline  PipeDeadline
	writeDeadline PipeDeadline

	maxBuf int
}

// signalLocked swaps and closes the notify channel while the caller holds p.mu.
// The close is performed after releasing the lock to avoid waking waiters
// that immediately contend on p.mu.
func (p *pipe) signalLocked() {
	ch := p.notify
	p.notify = make(chan struct{})
	p.mu.Unlock()
	close(ch)
}

// signal wakes any goroutine waiting on p.notify (acquires p.mu).
func (p *pipe) signal() {
	p.mu.Lock()
	p.signalLocked()
}

// bufLen returns the amount of unread data in the buffer. Caller must hold p.mu.
func (p *pipe) bufLen() int {
	return len(p.buf) - p.off
}

// consumeLocked copies buffered data into b and compacts the buffer.
// Caller must hold p.mu. Returns bytes copied.
func (p *pipe) consumeLocked(b []byte) int {
	n := copy(b, p.buf[p.off:])
	p.off += n
	if p.off == len(p.buf) {
		p.buf = p.buf[:0]
		p.off = 0
	} else if p.off > p.maxBuf/2 {
		// Compact: shift remaining data to front to bound backing array growth
		remaining := copy(p.buf, p.buf[p.off:])
		p.buf = p.buf[:remaining]
		p.off = 0
	}
	return n
}

func (p *pipe) read(b []byte) (n int, err error) {
	for {
		select {
		case <-p.readDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		default:
		}

		p.mu.Lock()
		if p.bufLen() > 0 {
			n = p.consumeLocked(b)
			p.signalLocked() // unlocks p.mu, wakes writer waiting for space
			return n, nil
		}
		if isClosedChan(p.done) {
			p.mu.Unlock()
			return 0, p.readCloseError()
		}
		ch := p.notify
		p.mu.Unlock()

		select {
		case <-ch:
		case <-p.done:
			// Drain remaining buffer
			p.mu.Lock()
			if p.bufLen() > 0 {
				n = p.consumeLocked(b)
				p.mu.Unlock()
				return n, nil
			}
			p.mu.Unlock()
			return 0, p.readCloseError()
		case <-p.readDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (p *pipe) closeRead(err error) error {
	if err == nil {
		err = io.ErrClosedPipe
	}
	p.rerr.Store(err)
	p.once.Do(func() { close(p.done) })
	p.signal()
	return nil
}

func (p *pipe) write(b []byte) (n int, err error) {
	select {
	case <-p.done:
		return 0, p.writeCloseError()
	case <-p.writeDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}

	for len(b) > 0 {
		p.mu.Lock()
		if p.bufLen() < p.maxBuf {
			space := p.maxBuf - p.bufLen()
			toWrite := min(len(b), space)
			p.buf = append(p.buf, b[:toWrite]...)
			b = b[toWrite:]
			n += toWrite
			p.signalLocked() // unlocks p.mu, wakes reader
			continue
		}
		if isClosedChan(p.done) {
			p.mu.Unlock()
			return n, p.writeCloseError()
		}
		ch := p.notify
		p.mu.Unlock()

		select {
		case <-ch:
		case <-p.done:
			return n, p.writeCloseError()
		case <-p.writeDeadline.Wait():
			return n, os.ErrDeadlineExceeded
		}
	}
	return n, nil
}

func (p *pipe) closeWrite(err error) error {
	if err == nil {
		err = io.EOF
	}
	p.werr.Store(err)
	p.once.Do(func() { close(p.done) })
	p.signal()
	return nil
}

func (p *pipe) readCloseError() error {
	rerr := p.rerr.Load()
	if werr := p.werr.Load(); rerr == nil && werr != nil {
		return werr
	}
	return io.ErrClosedPipe
}

func (p *pipe) writeCloseError() error {
	werr := p.werr.Load()
	if rerr := p.rerr.Load(); werr == nil && rerr != nil {
		return rerr
	}
	return io.ErrClosedPipe
}

type PipeReader struct{ *pipe }

func (r *PipeReader) Read(data []byte) (n int, err error) {
	return r.read(data)
}

func (r *PipeReader) Close() error {
	return r.CloseWithError(nil)
}

func (r *PipeReader) CloseWithError(err error) error {
	return r.closeRead(err)
}

type PipeWriter struct{ r *PipeReader }

func (w *PipeWriter) Write(data []byte) (n int, err error) {
	return w.r.write(data)
}

func (w *PipeWriter) Close() error {
	return w.CloseWithError(nil)
}

func (w *PipeWriter) CloseWithError(err error) error {
	return w.r.closeWrite(err)
}

func Pipe() (*PipeReader, *PipeWriter) {
	p := &pipe{
		done:          make(chan struct{}),
		notify:        make(chan struct{}),
		readDeadline:  MakePipeDeadline(),
		writeDeadline: MakePipeDeadline(),
		maxBuf:        defaultBufSize,
	}
	r := &PipeReader{p}
	w := &PipeWriter{r: r}
	return r, w
}

func (p *PipeReader) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.done) {
		return io.ErrClosedPipe
	}
	p.readDeadline.Set(t)
	return nil
}

func (p *PipeWriter) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.r.done) {
		return io.ErrClosedPipe
	}
	p.r.writeDeadline.Set(t)
	return nil
}
