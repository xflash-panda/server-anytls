package service

import (
	"sync"
	"testing"
)

// TestGetTrafficItemConcurrentNoLostTraffic verifies that concurrent calls to
// GetTrafficItem for the same userId do not lose traffic updates.
// Bug: GetTrafficItem uses load-then-store (not atomic), so concurrent first
// accesses can create multiple TrafficItems — only the last one survives,
// losing earlier traffic updates.
func TestGetTrafficItemConcurrentNoLostTraffic(t *testing.T) {
	s := &UsersService{
		trafficManager: newTrafficManager(),
	}

	const goroutines = 100
	const userId = 42

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// All goroutines get the TrafficItem and add 1 to Up.
	// If GetTrafficItem is race-free, all 100 adds land on the same item.
	for range goroutines {
		go func() {
			defer wg.Done()
			item := s.GetTrafficItem(userId)
			item.Up.Add(1)
		}()
	}
	wg.Wait()

	item := s.GetTrafficItem(userId)
	got := item.Up.Load()
	if got != goroutines {
		t.Fatalf("expected Up=%d, got %d (lost %d updates due to race)",
			goroutines, got, goroutines-got)
	}
}
