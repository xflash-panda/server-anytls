package server

import (
	"context"
	"fmt"

	pb "github.com/xflash-panda/server-agent-proto/pkg"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	"github.com/spf13/viper"
)

// initializeFromOptions fills server config and dependencies using Options when
// they are not already set.
func (s *Server) initializeFromOptions() error {
	if s.opts == nil {
		return fmt.Errorf("missing server options for initialization")
	}

	// Fetch node configuration from agent
	ctx, cancel := context.WithTimeout(context.Background(), service.DefaultTimeout)
	defer cancel()
	r, err := s.opts.AgentClient.Config(ctx, &pb.ConfigRequest{NodeId: int32(s.opts.ServiceConfig.NodeID), NodeType: pb.NodeType_ANYTLS})
	if err != nil {
		return fmt.Errorf("failed to fetch config from agent: %w", err)
	}
	nodeCfg, err := api.UnmarshalAnyTLSConfig(r.GetRawData())
	if err != nil {
		return fmt.Errorf("failed to unmarshal node config: %w", err)
	}
	s.anyTLSConfig = nodeCfg

	// Load TLS config
	tlsCfg, err := s.opts.CertConfig.Load()
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}
	s.tlsConfig = tlsCfg

	// Load extended config if provided
	if s.opts.ExtConfPath != "" {
		viper.SetConfigFile(s.opts.ExtConfPath)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read ext config: %w", err)
		}
		var extCfg *ExtConfig
		if err := viper.Unmarshal(&extCfg); err != nil {
			return fmt.Errorf("failed to unmarshal ext config: %w", err)
		}
		s.extConfig = extCfg
	}

	// Initialize user service
	s.userService = service.NewUsersService(s.opts.ServiceConfig, s.opts.AgentClient)
	s.userService.SetRegisterInfo(s.registerID)
	return nil
}
