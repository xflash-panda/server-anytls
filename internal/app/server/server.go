package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	C "github.com/apernet/hysteria/core/v2/server"
	"github.com/apernet/hysteria/extras/v2/outbounds"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Server struct {
	// runtime objects
	tlsConfig    *tls.Config
	anyTLSConfig *api.AnyTLSConfig
	listener     net.Listener
	userService  *service.UsersService

	// startup inputs
	apiConfig     api.Config
	serviceConfig service.Config
	certConfig    CertConfig
	extConfPath   string
	extConfig     *ExtConfig
	outbound      C.Outbound
	// 服务器状态
	ctx     context.Context
	cancel  context.CancelFunc
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
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

func New(apiConfig api.Config, serviceConfig service.Config, certConfig CertConfig, extConfPath string) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		apiConfig:     apiConfig,
		serviceConfig: serviceConfig,
		certConfig:    certConfig,
		extConfPath:   extConfPath,
		ctx:           ctx,
		cancel:        cancel,
	}
	return s, nil
}

// 连接注册表：添加连接
func (s *Server) addConn(c net.Conn) {
	s.connsMu.Lock()
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[c] = struct{}{}
	s.connsMu.Unlock()
}

// 连接注册表：移除连接
func (s *Server) removeConn(c net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

// 关闭所有活动连接，返回尝试关闭的数量
func (s *Server) closeAllConns() int {
	s.connsMu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return len(conns)
}

func (s *Server) Start() error {
	// 初始化依赖：API 客户端、节点配置、TLS、扩展配置、用户服务
	apiClient := api.New(&s.apiConfig)

	nodeConf, err := apiClient.Config(api.NodeId(s.serviceConfig.NodeID), api.AnyTLS)
	if err != nil {
		return fmt.Errorf("failed to get node config: %w", err)
	}
	s.anyTLSConfig = nodeConf.(*api.AnyTLSConfig)
	logrus.WithFields(logrus.Fields{
		"node_id":   s.serviceConfig.NodeID,
		"node_info": nodeConf.String(),
	}).Info("Server configuration loaded successfully")

	tlsConfig, err := s.certConfig.Load()
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}
	s.tlsConfig = tlsConfig

	if s.extConfPath != "" {
		viper.SetConfigFile(s.extConfPath)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read ext config: %w", err)
		}
		var extConfig *ExtConfig
		if err := viper.Unmarshal(&extConfig); err != nil {
			return fmt.Errorf("failed to unmarshal ext config: %w", err)
		}
		s.extConfig = extConfig
	}

	s.userService = service.NewUsersService(&s.serviceConfig, apiClient)

	// 校验配置并完成启动期初始化
	if err := validateConfig(s.anyTLSConfig, s.userService, s.tlsConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if s.anyTLSConfig.AllowInsecure == 1 {
		s.tlsConfig.InsecureSkipVerify = true
	}

	var uOb outbounds.PluggableOutbound
	if s.extConfig != nil {
		var obs []outbounds.OutboundEntry
		if len(s.extConfig.Outbounds) == 0 {
			obs = []outbounds.OutboundEntry{{
				Name:     "default",
				Outbound: outbounds.NewDirectOutboundSimple(outbounds.DirectOutboundModeAuto),
			}}
		} else {
			obs = make([]outbounds.OutboundEntry, len(s.extConfig.Outbounds))
			for i, entry := range s.extConfig.Outbounds {
				if entry.Name == "" {
					return configError{Field: "outbounds.name", Err: errors.New("empty outbound name")}
				}
				var ob outbounds.PluggableOutbound
				var err error
				switch strings.ToLower(entry.Type) {
				case "direct":
					ob, err = serverConfigOutboundDirectToOutbound(entry.Direct)
				case "socks5":
					ob, err = serverConfigOutboundSOCKS5ToOutbound(entry.SOCKS5)
				case "http":
					ob, err = serverConfigOutboundHTTPToOutbound(entry.HTTP)
				default:
					err = configError{Field: "outbounds.type", Err: errors.New("unsupported outbound type")}
				}
				if err != nil {
					return err
				}
				obs[i] = outbounds.OutboundEntry{Name: entry.Name, Outbound: ob}
			}
		}
		gLoader := &GeoLoader{
			GeoIPFilename:   "",
			GeoSiteFilename: "",
			UpdateInterval:  geoDefaultUpdateInterval,
			DownloadFunc:    geoDownloadFunc,
			DownloadErrFunc: geoDownloadErrFunc,
		}

		if len(s.extConfig.ACL.Inline) > 0 {
			aclOutbound, err := outbounds.NewACLEngineFromString(strings.Join(s.extConfig.ACL.Inline, "\n"), obs, gLoader)
			if err != nil {
				return configError{Field: "acl.inline", Err: err}
			}
			uOb = aclOutbound
		} else {
			uOb = obs[0].Outbound
		}
	} else {
		uOb = outbounds.NewDirectOutboundSimple(outbounds.DirectOutboundModeAuto)
	}
	s.outbound = &outbounds.PluggableOutboundAdapter{PluggableOutbound: uOb}
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
		s.addConn(conn)
		go func(c net.Conn) {
			defer s.removeConn(c)
			handleTcpConnection(s.ctx, c, s)
		}(conn)
	}
}

func (s *Server) Close() error {
	s.cancel()
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}
	n := s.closeAllConns()
	logrus.WithField("active_conns_closed", n).Info("closing all active connections")
	if err := s.userService.Close(); err != nil {
		return fmt.Errorf("failed to close user service: %w", err)
	}
	logrus.Info("server stopped")
	return nil
}
