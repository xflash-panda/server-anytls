package service

import (
	"testing"

	api "github.com/xflash-panda/server-client/pkg"
)

// TestCompareUserListNilUserList verifies that compareUserList does not panic
// when s.userList is nil (e.g., if called before init completes).
func TestCompareUserListNilUserList(t *testing.T) {
	s := &UsersService{}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("compareUserList panicked with nil userList: %v", r)
		}
	}()

	newUsers := []api.User{{UUID: "test-uuid", ID: 1}}
	s.compareUserList(&newUsers)
}
