package pipe

import (
	"sync"
	"time"
)

type PipeDeadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{}
}

func MakePipeDeadline() PipeDeadline {
	return PipeDeadline{cancel: make(chan struct{})}
}

func (d *PipeDeadline) Set(t time.Time) {
	d.mu.Lock()

	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel
	}
	d.timer = nil

	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}

	if dur := time.Until(t); dur > 0 {

		if closed {
			d.cancel = make(chan struct{})
		}

		d.timer = time.AfterFunc(dur, func() {
			close(d.cancel)
		})
		return
	}

	if !closed {
		close(d.cancel)
	}
}

func (d *PipeDeadline) Wait() chan struct{} {
	d.mu.Lock()

	defer d.mu.Unlock()

	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {

	case <-c:
		return true
	default:
		return false
	}
}
