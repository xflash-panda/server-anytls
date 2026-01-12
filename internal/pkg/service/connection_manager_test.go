package service

import (
	"sync"
	"sync/atomic"
	"testing"
)

// mockCloser is a mock implementation of io.Closer for testing
type mockCloser struct {
	closed atomic.Bool
}

func (m *mockCloser) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockCloser) IsClosed() bool {
	return m.closed.Load()
}

func TestConnectionManager_AddAndRemove(t *testing.T) {
	cm := newConnectionManager()
	conn1 := &mockCloser{}
	conn2 := &mockCloser{}

	// Add connections for user 1
	cm.Add(1, conn1)
	cm.Add(1, conn2)

	// Verify connections are added
	cm.mu.RLock()
	if len(cm.conns[1]) != 2 {
		t.Errorf("expected 2 connections for user 1, got %d", len(cm.conns[1]))
	}
	cm.mu.RUnlock()

	// Remove one connection
	cm.Remove(1, conn1)

	cm.mu.RLock()
	if len(cm.conns[1]) != 1 {
		t.Errorf("expected 1 connection for user 1, got %d", len(cm.conns[1]))
	}
	cm.mu.RUnlock()

	// Remove the last connection - should clean up the user entry
	cm.Remove(1, conn2)

	cm.mu.RLock()
	if _, exists := cm.conns[1]; exists {
		t.Error("expected user 1 entry to be removed when no connections remain")
	}
	cm.mu.RUnlock()
}

func TestConnectionManager_CloseAll(t *testing.T) {
	cm := newConnectionManager()
	conn1 := &mockCloser{}
	conn2 := &mockCloser{}
	conn3 := &mockCloser{}

	// Add connections for user 1 and user 2
	cm.Add(1, conn1)
	cm.Add(1, conn2)
	cm.Add(2, conn3)

	// Close all connections for user 1
	cm.CloseAll(1)

	// Verify user 1's connections are closed
	if !conn1.IsClosed() {
		t.Error("conn1 should be closed")
	}
	if !conn2.IsClosed() {
		t.Error("conn2 should be closed")
	}

	// Verify user 2's connection is not affected
	if conn3.IsClosed() {
		t.Error("conn3 should not be closed")
	}

	// Verify user 1 is removed from the map
	cm.mu.RLock()
	if _, exists := cm.conns[1]; exists {
		t.Error("user 1 should be removed from connections map")
	}
	if _, exists := cm.conns[2]; !exists {
		t.Error("user 2 should still exist in connections map")
	}
	cm.mu.RUnlock()
}

func TestConnectionManager_CloseAllNonExistentUser(t *testing.T) {
	cm := newConnectionManager()

	// Should not panic when closing connections for non-existent user
	cm.CloseAll(999)
}

func TestConnectionManager_RemoveNonExistentConnection(t *testing.T) {
	cm := newConnectionManager()
	conn := &mockCloser{}

	// Should not panic when removing non-existent connection
	cm.Remove(1, conn)

	// Add a connection then try to remove a different one
	cm.Add(1, conn)
	otherConn := &mockCloser{}
	cm.Remove(1, otherConn)

	// Original connection should still exist
	cm.mu.RLock()
	if len(cm.conns[1]) != 1 {
		t.Errorf("expected 1 connection for user 1, got %d", len(cm.conns[1]))
	}
	cm.mu.RUnlock()
}

func TestConnectionManager_Concurrent(t *testing.T) {
	cm := newConnectionManager()
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(userId int) {
			defer wg.Done()
			conn := &mockCloser{}
			cm.Add(userId%10, conn)
		}(i)
	}

	wg.Wait()

	// Concurrent removes and closes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(userId int) {
			defer wg.Done()
			cm.CloseAll(userId)
		}(i)
	}

	wg.Wait()

	// All users should be removed
	cm.mu.RLock()
	if len(cm.conns) != 0 {
		t.Errorf("expected 0 users, got %d", len(cm.conns))
	}
	cm.mu.RUnlock()
}

func TestConnectionManager_MultipleUsersMultipleConnections(t *testing.T) {
	cm := newConnectionManager()

	// Create connections for multiple users
	users := make(map[int][]*mockCloser)
	for userId := 1; userId <= 3; userId++ {
		for i := 0; i < 5; i++ {
			conn := &mockCloser{}
			users[userId] = append(users[userId], conn)
			cm.Add(userId, conn)
		}
	}

	// Verify all connections are registered
	cm.mu.RLock()
	for userId := 1; userId <= 3; userId++ {
		if len(cm.conns[userId]) != 5 {
			t.Errorf("expected 5 connections for user %d, got %d", userId, len(cm.conns[userId]))
		}
	}
	cm.mu.RUnlock()

	// Close user 2's connections
	cm.CloseAll(2)

	// Verify only user 2's connections are closed
	for userId, conns := range users {
		for i, conn := range conns {
			if userId == 2 {
				if !conn.IsClosed() {
					t.Errorf("user 2 conn %d should be closed", i)
				}
			} else {
				if conn.IsClosed() {
					t.Errorf("user %d conn %d should not be closed", userId, i)
				}
			}
		}
	}
}
