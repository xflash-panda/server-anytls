package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	pb "github.com/xflash-panda/server-agent-proto/pkg"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	"github.com/sirupsen/logrus"
)

// Outbound is the interface for establishing outbound connections.
// This matches the interface expected by outbound_tcp.go.
type Outbound interface {
	TCP(addr string) (net.Conn, error)
	UDP(addr string) (UDPConn, error)
}

type Server struct {
	tlsConfig    *tls.Config
	anyTLSConfig *api.AnyTLSConfig
	listeners    []net.Listener
	userService  *service.UsersService
	extConfig    *ExtConfig
	outbound     Outbound
	opts         *Options
	registerID   string
	// 服务器状态
	ctx    context.Context
	cancel context.CancelFunc
}

func validateConfig(anyTLSConfig *api.AnyTLSConfig, userService *service.UsersService, tlsConfig *tls.Config) error {
	if anyTLSConfig == nil {
		return errors.New("anyTLSConfig is nil")
	}
	if userService == nil {
		return errors.New("userService is nil")
	}
	if tlsConfig == nil {
		return errors.New("tlsConfig is nil")
	}
	if anyTLSConfig.ServerPort <= 0 || anyTLSConfig.ServerPort > 65535 {
		return fmt.Errorf("invalid server port: %d", anyTLSConfig.ServerPort)
	}
	return nil
}

func New(opts *Options) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		opts:   opts,
		ctx:    ctx,
		cancel: cancel,
	}
	return s, nil
}

// checkAndRegister checks if registration is needed and performs registration if necessary.
// Returns the register ID to use (either from existing state or newly registered).
func (s *Server) checkAndRegister() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), service.DefaultTimeout)
	defer cancel()

	// Try to load existing state
	existingState, err := LoadState(s.opts.DataDir)
	if err != nil {
		logrus.WithError(err).Warn("failed to load state")
	}

	// Check if we need to re-register (no existing state or node ID changed)
	hostName, _ := os.Hostname()
	needRegister := existingState == nil ||
		existingState.RegisterID == ""

	if !needRegister {
		// Verify existing register ID with agent
		verifyResp, err := s.opts.AgentClient.Verify(ctx, &pb.VerifyRequest{
			NodeType:   pb.NodeType_ANYTLS,
			RegisterId: existingState.RegisterID,
		})
		if err != nil {
			logrus.WithError(err).Warn("failed to verify existing register ID, will re-register")
			// Clear state and re-register
			if err := ClearState(s.opts.DataDir); err != nil {
				logrus.WithError(err).Warn("failed to clear state")
			}
			// Fall through to registration
		} else if verifyResp != nil && verifyResp.GetResult() {
			// Register ID is valid, use it
			logrus.WithFields(logrus.Fields{
				"register_id": existingState.RegisterID,
				"node_id":     existingState.NodeID,
				"hostname":    existingState.Hostname,
			}).Info("verified and loaded register ID from state")
			return existingState.RegisterID, nil
		} else {
			// Verify failed (result is false)
			logrus.Warn("verify failed: result is not true, will re-register")
			// Clear state and re-register
			if err := ClearState(s.opts.DataDir); err != nil {
				logrus.WithError(err).Warn("failed to clear state")
			}
			// Fall through to registration
		}
	}

	// Need to register - first fetch config to get server port
	r, err := s.opts.AgentClient.Config(ctx, &pb.ConfigRequest{NodeId: int32(s.opts.ServiceConfig.NodeID), NodeType: pb.NodeType_ANYTLS})
	if err != nil {
		return "", fmt.Errorf("failed to fetch config from agent: %w", err)
	}
	nodeCfg, err := api.UnmarshalAnyTLSConfig(r.GetRawData())
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal node config: %w", err)
	}

	// Register node to get register_id (agent internally manages register mapping)
	portStr := strconv.Itoa(nodeCfg.ServerPort)
	ipStr := ""
	regResp, err := s.opts.AgentClient.Register(ctx, &pb.RegisterRequest{
		NodeId:   int32(s.opts.ServiceConfig.NodeID),
		NodeType: pb.NodeType_ANYTLS,
		HostName: hostName,
		Port:     portStr,
		Ip:       ipStr,
	})
	if err != nil {
		return "", fmt.Errorf("failed to register node: %w", err)
	}

	registerID := regResp.GetRegisterId()

	// Save state
	newState := &State{
		RegisterID: registerID,
		NodeID:     s.opts.ServiceConfig.NodeID,
		Hostname:   hostName,
	}
	if err := SaveState(s.opts.DataDir, newState); err != nil {
		logrus.WithError(err).Warn("failed to save state")
	} else {
		logrus.WithFields(logrus.Fields{
			"register_id": registerID,
			"node_id":     s.opts.ServiceConfig.NodeID,
			"hostname":    hostName,
		}).Info("saved state after registration")
	}

	return registerID, nil
}

func (s *Server) Start() error {
	// Check and register first (independent of service initialization)
	registerID, err := s.checkAndRegister()
	if err != nil {
		return err
	}
	s.registerID = registerID

	// Initialize configuration and services if not initialized yet
	if s.anyTLSConfig == nil || s.tlsConfig == nil || s.userService == nil {
		if err := s.initializeFromOptions(); err != nil {
			return err
		}
	}

	if err := validateConfig(s.anyTLSConfig, s.userService, s.tlsConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Build outbound and ACL engine
	if err := s.buildOutbound(); err != nil {
		return err
	}

	if s.anyTLSConfig.AllowInsecure == 1 {
		s.tlsConfig.InsecureSkipVerify = true
	}
	listeners, err := listenDualStack(s.anyTLSConfig.ServerPort)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listeners = listeners
	for _, ln := range listeners {
		logrus.WithFields(logrus.Fields{
			"addr": ln.Addr().String(),
		}).Info("Server listening")
	}
	if len(s.anyTLSConfig.PaddingRules) > 0 {
		paddingRules := []byte(s.anyTLSConfig.PaddingRules)
		if padding.UpdatePaddingScheme(paddingRules) {
			logrus.Infoln("loaded padding scheme: ", s.anyTLSConfig.PaddingRules)
		} else {
			logrus.Errorln("failed to load padding scheme: ", s.anyTLSConfig.PaddingRules)
		}
	}
	if err := s.userService.Start(); err != nil {
		return fmt.Errorf("failed to start user service: %w", err)
	}
	logrus.Infoln("user service started")
	logrus.Infoln("start accepting connections")

	var wg sync.WaitGroup
	for _, ln := range s.listeners {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			for {
				conn, err := l.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					logrus.WithError(err).Error("failed to accept connection")
					continue
				}
				go handleTcpConnection(s.ctx, conn, s)
			}
		}(ln)
	}
	wg.Wait()
	return nil
}

func listenDualStack(port int) ([]net.Listener, error) {
	networks := []struct {
		net  string
		addr string
	}{
		{"tcp4", fmt.Sprintf("0.0.0.0:%d", port)},
		{"tcp6", fmt.Sprintf("[::]:%d", port)},
	}
	var listeners []net.Listener
	for _, n := range networks {
		ln, err := net.Listen(n.net, n.addr)
		if err != nil {
			logrus.WithError(err).Warnf("failed to listen on %s %s", n.net, n.addr)
			continue
		}
		listeners = append(listeners, ln)
	}
	if len(listeners) == 0 {
		return nil, fmt.Errorf("failed to listen on any address for port %d", port)
	}
	return listeners, nil
}

func (s *Server) Close() error {
	// Unregister first with agent to prioritize deregistration
	if s.opts != nil && s.opts.AgentClient != nil && s.registerID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), service.DefaultTimeout)
		defer cancel()
		resp, err := s.opts.AgentClient.Unregister(ctx, &pb.UnregisterRequest{NodeType: pb.NodeType_ANYTLS, RegisterId: s.registerID})
		if err != nil {
			logrus.WithError(err).Error("failed to unregister with agent")
		} else if resp != nil && resp.GetResult() {
			logrus.Info("unregistered from agent")
			// Clear state only after successful unregister
			if err := ClearState(s.opts.DataDir); err != nil {
				logrus.WithError(err).Error("failed to clear state")
			}
		} else {
			logrus.Error("unregister failed: result is not true")
		}
	}
	s.cancel()
	for _, ln := range s.listeners {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}
	if s.userService != nil {
		if err := s.userService.Close(); err != nil {
			return fmt.Errorf("failed to close user service: %w", err)
		}
	}
	logrus.Info("server stopped")
	return nil
}
