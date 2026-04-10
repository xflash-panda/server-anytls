package pipe

import (
	"sync"
	"sync/atomic"
	"time"
)

type PipeDeadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel atomic.Pointer[chan struct{}]
}

func MakePipeDeadline() PipeDeadline {
	d := PipeDeadline{}
	ch := make(chan struct{})
	d.cancel.Store(&ch)
	return d //nolint:govet // value assigned to struct field, used via pointer receiver only
}

func (d *PipeDeadline) Set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	cancel := *d.cancel.Load()
	if d.timer != nil && !d.timer.Stop() {
		<-cancel
	}
	d.timer = nil

	closed := isClosedChan(cancel)
	if t.IsZero() {
		if closed {
			ch := make(chan struct{})
			d.cancel.Store(&ch)
		}
		return
	}

	if dur := time.Until(t); dur > 0 {
		if closed {
			ch := make(chan struct{})
			d.cancel.Store(&ch)
		}
		d.timer = time.AfterFunc(dur, func() {
			close(*d.cancel.Load())
		})
		return
	}

	if !closed {
		close(cancel)
	}
}

// Wait returns the cancel channel using an atomic load (lock-free).
func (d *PipeDeadline) Wait() chan struct{} {
	return *d.cancel.Load()
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
