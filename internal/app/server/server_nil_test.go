package server

import (
	"crypto/tls"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/service"
)

// fakeNodeConfig is a non-AnyTLSConfig implementation of api.NodeConfig.
type fakeNodeConfig struct{}

func (f *fakeNodeConfig) String() string   { return "fake" }
func (f *fakeNodeConfig) TypeName() string { return "fake" }

// TestValidateConfigWrongType verifies that validateConfig does not panic
// when nodeConfig is not *api.AnyTLSConfig.
func TestValidateConfigWrongType(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("validateConfig panicked with wrong NodeConfig type: %v", r)
		}
	}()

	us := service.NewUsersServiceWithTrafficManager(service.NewExportedTrafficManager())
	err := validateConfig(&fakeNodeConfig{}, us, &tls.Config{})
	if err == nil {
		t.Fatal("expected error for wrong NodeConfig type, got nil")
	}
}
