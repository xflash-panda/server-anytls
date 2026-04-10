package service

import (
	"sync"
	"testing"
)

// TestTrafficItemConcurrentAddIntegrity verifies that concurrent Add calls on
// TrafficItem do not lose updates. This guards the invariant that switching from
// *Counter to atomic.Uint64 preserves correctness.
func TestTrafficItemConcurrentAddIntegrity(t *testing.T) {
	item := newTrafficItem()

	const goroutines = 200
	const addsPerGoroutine = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines * 3) // 3 fields: Up, Down, Count

	for range goroutines {
		go func() {
			defer wg.Done()
			for range addsPerGoroutine {
				item.Up.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			for range addsPerGoroutine {
				item.Down.Add(2)
			}
		}()
		go func() {
			defer wg.Done()
			for range addsPerGoroutine {
				item.Count.Add(3)
			}
		}()
	}
	wg.Wait()

	expectedUp := uint64(goroutines * addsPerGoroutine * 1)
	expectedDown := uint64(goroutines * addsPerGoroutine * 2)
	expectedCount := uint64(goroutines * addsPerGoroutine * 3)

	if item.Up.Load() != expectedUp {
		t.Fatalf("Up: expected %d, got %d", expectedUp, item.Up.Load())
	}
	if item.Down.Load() != expectedDown {
		t.Fatalf("Down: expected %d, got %d", expectedDown, item.Down.Load())
	}
	if item.Count.Load() != expectedCount {
		t.Fatalf("Count: expected %d, got %d", expectedCount, item.Count.Load())
	}
}

// TestTrafficItemResetClearsAll verifies that reset/delete zeroes all counters.
func TestTrafficItemResetClearsAll(t *testing.T) {
	item := newTrafficItem()
	item.Up.Add(100)
	item.Down.Add(200)
	item.Count.Add(300)

	item.delete()

	if item.Up.Load() != 0 {
		t.Fatalf("Up should be 0 after reset, got %d", item.Up.Load())
	}
	if item.Down.Load() != 0 {
		t.Fatalf("Down should be 0 after reset, got %d", item.Down.Load())
	}
	if item.Count.Load() != 0 {
		t.Fatalf("Count should be 0 after reset, got %d", item.Count.Load())
	}
}

// TestTrafficManagerLoadOrStoreConcurrent verifies that concurrent loadOrStore
// calls for the same userId always return the same TrafficItem.
func TestTrafficManagerLoadOrStoreConcurrent(t *testing.T) {
	tm := newTrafficManager()
	const goroutines = 100
	const userId = 42

	items := make([]*TrafficItem, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			items[idx] = tm.loadOrStore(userId)
		}(i)
	}
	wg.Wait()

	// All goroutines must have gotten the same pointer
	for i := 1; i < goroutines; i++ {
		if items[i] != items[0] {
			t.Fatalf("goroutine %d got different TrafficItem pointer", i)
		}
	}
}

// TestTrafficManagerToUserTrafficsFiltersZero verifies that toUserTraffics
// excludes entries with all-zero counters.
func TestTrafficManagerToUserTrafficsFiltersZero(t *testing.T) {
	tm := newTrafficManager()

	// User with traffic
	item1 := tm.loadOrStore(1)
	item1.Up.Add(100)

	// User with zero traffic
	tm.loadOrStore(2)

	traffics := tm.toUserTraffics()
	if len(traffics) != 1 {
		t.Fatalf("expected 1 user traffic, got %d", len(traffics))
	}
	if traffics[0].UID != 1 {
		t.Fatalf("expected UID=1, got %d", traffics[0].UID)
	}
	if traffics[0].Upload != 100 {
		t.Fatalf("expected Upload=100, got %d", traffics[0].Upload)
	}
}

// TestTrafficManagerClearResetsAll verifies that clear resets all counters.
func TestTrafficManagerClearResetsAll(t *testing.T) {
	tm := newTrafficManager()

	item := tm.loadOrStore(1)
	item.Up.Add(100)
	item.Down.Add(200)
	item.Count.Add(300)

	tm.clear()

	if item.Up.Load() != 0 || item.Down.Load() != 0 || item.Count.Load() != 0 {
		t.Fatal("clear did not reset counters")
	}
}

// BenchmarkTrafficItemAdd benchmarks the hot-path atomic Add on TrafficItem.
func BenchmarkTrafficItemAdd(b *testing.B) {
	item := newTrafficItem()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			item.Up.Add(1)
		}
	})
}
