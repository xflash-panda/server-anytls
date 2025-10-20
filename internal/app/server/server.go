package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/xflash-panda/server-agent-proto/pkg"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	C "github.com/apernet/hysteria/core/v2/server"
	"github.com/sirupsen/logrus"
)

type Server struct {
	tlsConfig    *tls.Config
	anyTLSConfig *api.AnyTLSConfig
	listener     net.Listener
	userService  *service.UsersService
	extConfig    *ExtConfig
	outbound     C.Outbound
	opts         *Options
	registerID   int32
	// 服务器状态
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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

func (s *Server) Start() error {
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
	addr := fmt.Sprintf(":%d", s.anyTLSConfig.ServerPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener
	logrus.WithFields(logrus.Fields{
		"addr": addr,
	}).Info("Server listening on TCP")
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
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			logrus.WithError(err).Error("failed to accept connection")
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			handleTcpConnection(s.ctx, conn, s)
		}()
	}
}

func (s *Server) Close() error {
	// Unregister first with agent to prioritize deregistration
	if s.opts != nil && s.opts.AgentClient != nil && s.registerID > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), service.DefaultTimeout)
		defer cancel()
		if _, err := s.opts.AgentClient.Unregister(ctx, &pb.UnregisterRequest{NodeType: pb.NodeType_ANYTLS, RegisterId: s.registerID}); err != nil {
			logrus.WithError(err).Warn("failed to unregister with agent")
		} else {
			logrus.Info("unregistered from agent")
		}
	}
	s.cancel()
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		logrus.Info("all connections closed")
	case <-time.After(30 * time.Second):
		logrus.Warn("timeout waiting for connections to close")
	}
	if err := s.userService.Close(); err != nil {
		return fmt.Errorf("failed to close user service: %w", err)
	}
	logrus.Info("server stopped")
	return nil
}
