package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"
)

type Server struct {
	tlsConfig    *tls.Config
	anyTLSConfig *api.AnyTLSConfig
	listener     net.Listener
	userService  *service.UsersService

	// 服务器状态
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func validateConfig(nodeConfig api.NodeConfig, userService *service.UsersService, tlsConfig *tls.Config) error {
	if nodeConfig == nil {
		return errors.New("nodeConfig is nil")
	}
	if userService == nil {
		return errors.New("userService is nil")
	}
	if tlsConfig == nil {
		return errors.New("tlsConfig is nil")
	}
	anyTLSConfig := nodeConfig.(*api.AnyTLSConfig)
	if anyTLSConfig.ServerPort <= 0 || anyTLSConfig.ServerPort > 65535 {
		return fmt.Errorf("invalid server port: %d", anyTLSConfig.ServerPort)
	}
	return nil
}

func New(nodeConfig api.NodeConfig, userService *service.UsersService, tlsConfig *tls.Config) (*Server, error) {
	if err := validateConfig(nodeConfig, userService, tlsConfig); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		anyTLSConfig: nodeConfig.(*api.AnyTLSConfig),
		tlsConfig:    tlsConfig,
		userService:  userService,
		ctx:          ctx,
		cancel:       cancel,
	}
	if s.anyTLSConfig.AllowInsecure == 1 {
		s.tlsConfig.InsecureSkipVerify = true
	}
	return s, nil
}

func (s *Server) Run() error {
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

func (s *Server) Stop() error {
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
