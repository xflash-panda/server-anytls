package service

import (
	"sync"
	"testing"

	api "github.com/xflash-panda/server-client/pkg"
)

// TestCompareUserListNilNewUsers verifies that compareUserList does not panic
// when newUsers parameter is nil.
// Bug: newUsers is dereferenced as *newUsers without nil check at line 208.
func TestCompareUserListNilNewUsers(t *testing.T) {
	s := &UsersService{}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("compareUserList panicked with nil newUsers: %v", r)
		}
	}()

	deleted, added := s.compareUserList(nil)
	if len(deleted) != 0 || len(added) != 0 {
		t.Fatalf("expected empty results, got deleted=%d added=%d", len(deleted), len(added))
	}
}

// TestDrainUserTrafficsBadTypeInMap verifies that drainUserTraffics does not
// panic when a non-*TrafficItem value is stored in the sync.Map.
// Bug: value.(*TrafficItem) at line 286 has no comma-ok guard.
func TestDrainUserTrafficsBadTypeInMap(t *testing.T) {
	tm := newTrafficManager()

	// Intentionally store a wrong type
	tm.store.Store(1, "not-a-traffic-item")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("drainUserTraffics panicked on bad type: %v", r)
		}
	}()

	result := tm.drainUserTraffics()
	// Should skip the bad entry gracefully
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %d entries", len(result))
	}
}

// TestLoadOrStoreBadTypeInMap verifies that loadOrStore does not panic
// when a non-*TrafficItem value already exists in the sync.Map.
// Bug: item.(*TrafficItem) at lines 305/308 has no comma-ok guard.
func TestLoadOrStoreBadTypeInMap(t *testing.T) {
	tm := newTrafficManager()

	// Pre-store a wrong type
	tm.store.Store(42, "bad-value")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("loadOrStore panicked on bad type: %v", r)
		}
	}()

	item := tm.loadOrStore(42)
	if item == nil {
		t.Fatal("expected non-nil TrafficItem after recovery from bad type")
	}
}

// TestDrainUserTrafficsBadTypeSkipsEntry verifies correct entries are still
// collected when a bad entry is mixed in.
func TestDrainUserTrafficsBadTypeSkipsEntry(t *testing.T) {
	tm := newTrafficManager()

	// Store one valid entry
	goodItem := newTrafficItem()
	goodItem.Up.Store(100)
	tm.store.Store(1, goodItem)

	// Store one bad entry
	tm.store.Store(2, "bad-value")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("drainUserTraffics panicked: %v", r)
		}
	}()

	result := tm.drainUserTraffics()
	// Should have collected the good entry
	found := false
	for _, ut := range result {
		if ut.UID == 1 && ut.Upload == 100 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected to find valid traffic entry for uid=1")
	}
}

// TestCompareUserListBothNil verifies compare works correctly with
// both nil userList and nil newUsers at the same time.
func TestCompareUserListBothNil(t *testing.T) {
	s := &UsersService{userList: nil}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("compareUserList panicked: %v", r)
		}
	}()

	deleted, added := s.compareUserList(nil)
	if len(deleted) != 0 || len(added) != 0 {
		t.Fatalf("expected empty results")
	}
}

// TestCompareUserListNilNewUsersWithExistingUsers verifies no panic when
// newUsers is nil but existing userList has users.
func TestCompareUserListNilNewUsersWithExistingUsers(t *testing.T) {
	existing := []api.User{{UUID: "user-1", ID: 1}, {UUID: "user-2", ID: 2}}
	s := &UsersService{userList: &existing}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("compareUserList panicked: %v", r)
		}
	}()

	deleted, added := s.compareUserList(nil)
	// All existing users should be in deleted
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted users, got %d", len(deleted))
	}
	if len(added) != 0 {
		t.Fatalf("expected 0 added users, got %d", len(added))
	}
}

// TestDrainConcurrentWithBadType verifies no panic under concurrent access
// when bad types are in the map.
func TestDrainConcurrentWithBadType(t *testing.T) {
	tm := newTrafficManager()
	tm.store.Store(1, "bad")
	tm.store.Store(2, newTrafficItem())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("drainUserTraffics panicked: %v", r)
				}
			}()
			tm.drainUserTraffics()
		}()
	}
	wg.Wait()
}
