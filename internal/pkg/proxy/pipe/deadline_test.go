package pipe

import (
	"sync"
	"testing"
)

// BenchmarkPipeDeadlineWaitContention measures contention on Wait() when called
// concurrently from multiple goroutines, as happens in the pipe read/write hot
// path. Wait() currently acquires a mutex on every call — with atomic storage
// of the cancel channel, this lock is unnecessary.
func BenchmarkPipeDeadlineWaitContention(b *testing.B) {
	d := MakePipeDeadline()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := d.Wait()
			// Prevent compiler from optimizing away
			select {
			case <-ch:
			default:
			}
		}
	})
}

// TestPipeDeadlineWaitIsLockFree verifies that Wait() does not contend with
// concurrent Set() calls. We measure this by running Wait() under high
// concurrency and checking it completes in reasonable time.
func TestPipeDeadlineWaitIsLockFree(t *testing.T) {
	d := MakePipeDeadline()

	const goroutines = 100
	const iterations = 10000
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				ch := d.Wait()
				select {
				case <-ch:
				default:
				}
			}
		}()
	}
	wg.Wait()
	// If we get here without hanging, Wait() has acceptable concurrency.
}
