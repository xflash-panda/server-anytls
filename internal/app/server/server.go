package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

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
	s := &Server{
		anyTLSConfig: nodeConfig.(*api.AnyTLSConfig),
		tlsConfig:    tlsConfig,
		userService:  userService,
	}
	if s.anyTLSConfig.AllowInsecure == 1 {
		s.tlsConfig.InsecureSkipVerify = true
	}

	return s, nil
}

func (s *Server) Run() {
	addr := fmt.Sprintf(":%d", s.anyTLSConfig.ServerPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logrus.Fatalln("listen:", err)
	}
	s.listener = listener

	logrus.WithFields(logrus.Fields{
		"port": s.anyTLSConfig.ServerPort,
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
	logrus.Infoln("start user service")
	s.userService.Start()

	ctx := context.Background()
	logrus.Infoln("start accept connections")
	for {
		c, err := listener.Accept()
		if err != nil {
			logrus.Fatalln("accept:", err)
		}
		go handleTcpConnection(ctx, c, s)
	}
}

func (s *Server) Stop() error {
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
		logrus.Info("Server stopped")
	}
	s.userService.Close()
	return nil
}
