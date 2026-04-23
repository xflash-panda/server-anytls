package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Server struct {
	// runtime objects
	tlsConfig    *tls.Config
	anyTLSConfig *api.AnyTLSConfig
	listeners    []net.Listener
	userService  *service.UsersService
	apiClient    *api.Client
	registerID   string

	// startup inputs
	apiConfig     api.Config
	serviceConfig service.Config
	certConfig    CertConfig
	aclConfPath   string
	extConfig     *ExtConfig
	outbound      Outbound
	dataDir       string // 数据文件目录
	// 服务器状态
	ctx    context.Context
	cancel context.CancelFunc
	// shutdown state
	closeOnce sync.Once
	closeErr  error
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

	anyTLSConfig, ok := nodeConfig.(*api.AnyTLSConfig)
	if !ok {
		return fmt.Errorf("unexpected node config type: %T", nodeConfig)
	}
	if anyTLSConfig.ServerPort <= 0 || anyTLSConfig.ServerPort > 65535 {
		return fmt.Errorf("invalid server port: %d", anyTLSConfig.ServerPort)
	}
	return nil
}

func New(apiConfig api.Config, serviceConfig service.Config, certConfig CertConfig, aclConfPath string, dataDir string) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	if dataDir == "" {
		dataDir = DefaultDataDir
	}
	s := &Server{
		apiConfig:     apiConfig,
		serviceConfig: serviceConfig,
		certConfig:    certConfig,
		aclConfPath:   aclConfPath,
		dataDir:       dataDir,
		ctx:           ctx,
		cancel:        cancel,
	}
	return s, nil
}

func (s *Server) Start() error {
	// 初始化依赖：API 客户端、节点配置、TLS、扩展配置、用户服务
	apiClient := api.New(&s.apiConfig)
	s.apiClient = apiClient

	// 获取主机名
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	// 尝试加载已保存的状态
	var registerID string
	savedState, err := LoadState(s.dataDir)
	if err != nil {
		logrus.Warnf("failed to load state: %s", err)
	}

	// 第一步：先获取节点配置信息
	logrus.Infof("Fetching node config for NodeID: %d", s.serviceConfig.NodeID)
	nodeConf, err := apiClient.Config(s.ctx, api.NodeId(s.serviceConfig.NodeID), api.AnyTLS)
	if err != nil {
		return fmt.Errorf("failed to get node config: %w", err)
	}
	anyTLSConf, ok := nodeConf.(*api.AnyTLSConfig)
	if !ok {
		return fmt.Errorf("unexpected node config type: %T", nodeConf)
	}
	s.anyTLSConfig = anyTLSConf
	logrus.WithFields(logrus.Fields{
		"node_id":   s.serviceConfig.NodeID,
		"node_info": nodeConf.String(),
	}).Info("Server configuration loaded successfully")

	// 第二步：验证并复用已保存的 register_id
	if savedState != nil && savedState.RegisterID != "" && savedState.NodeID == s.serviceConfig.NodeID {
		logrus.Infof("Found saved registerID: %s, verifying...", savedState.RegisterID)

		// 调用 Verify 接口验证 register_id 是否有效
		isValid, err := apiClient.Verify(s.ctx, savedState.RegisterID, api.AnyTLS)
		if err != nil {
			logrus.Warnf("failed to verify registerID: %s, will re-register", err)
			// 验证失败，清空状态文件
			if clearErr := ClearState(s.dataDir); clearErr != nil {
				logrus.Warnf("failed to clear state: %s", clearErr)
			}
		} else if isValid {
			// register_id 仍然有效，直接复用
			logrus.Infof("registerID is valid, reusing it")
			registerID = savedState.RegisterID
		} else {
			// register_id 无效，清空状态文件
			logrus.Infof("registerID is invalid, will re-register")
			if clearErr := ClearState(s.dataDir); clearErr != nil {
				logrus.Warnf("failed to clear state: %s", clearErr)
			}
		}
	}

	// 第三步：如果没有有效的 register_id，则进行注册
	if registerID == "" {
		logrus.Infof("Registering node with hostname: %s, ServerPort: %d", hostname, s.anyTLSConfig.ServerPort)
		registerID, err = apiClient.Register(s.ctx, api.NodeId(s.serviceConfig.NodeID), api.AnyTLS, hostname, s.anyTLSConfig.ServerPort, "")
		if err != nil {
			return fmt.Errorf("failed to register node: %w", err)
		}
		logrus.Infof("Registered with server, registerID: %s", registerID)

		// 注册成功后保存状态
		newState := &State{
			RegisterID: registerID,
			NodeID:     s.serviceConfig.NodeID,
			Hostname:   hostname,
		}
		if saveErr := SaveState(s.dataDir, newState); saveErr != nil {
			logrus.Warnf("failed to save state: %s", saveErr)
			// 不返回错误，因为注册成功了
		}
	}

	// 注册成功后立即保存到Server结构中，这样即使后续步骤失败，Close()也能取消注册
	s.registerID = registerID
	s.serviceConfig.RegisterID = registerID
	logrus.WithFields(logrus.Fields{
		"register_id": registerID,
	}).Info("Node registered successfully")

	tlsConfig, err := s.certConfig.Load()
	if err != nil {
		return fmt.Errorf("failed to load TLS config: %w", err)
	}
	s.tlsConfig = tlsConfig

	// Load ACL config if provided
	if s.aclConfPath != "" {
		confPath := s.aclConfPath
		// Convert relative path to absolute path based on executable location
		if !filepath.IsAbs(confPath) {
			execPath, err := os.Executable()
			if err == nil {
				confPath = filepath.Join(filepath.Dir(execPath), confPath)
			}
		}
		// Validate YAML format
		ext := filepath.Ext(confPath)
		if ext != ".yaml" && ext != ".yml" {
			return fmt.Errorf("ACL config file must be in YAML format (.yaml or .yml), got: %s", ext)
		}
		logrus.WithField("path", confPath).Info("loading ACL config")
		viper.SetConfigFile(confPath)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read ACL config: %w", err)
		}
		var extConfig *ExtConfig
		if err := viper.Unmarshal(&extConfig); err != nil {
			return fmt.Errorf("failed to unmarshal ACL config: %w", err)
		}
		s.extConfig = extConfig
		logrus.WithFields(logrus.Fields{
			"outbounds": len(extConfig.Outbounds),
			"acl_rules": len(extConfig.ACL.Inline),
		}).Info("ACL config loaded")
	} else {
		logrus.Info("no ACL config path provided")
	}

	s.userService = service.NewUsersService(&s.serviceConfig, apiClient)

	// 校验配置并完成启动期初始化
	if err := validateConfig(s.anyTLSConfig, s.userService, s.tlsConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if s.anyTLSConfig.AllowInsecure == 1 {
		s.tlsConfig.InsecureSkipVerify = true
	}

	// Build outbound using acl-engine
	if err := s.buildOutbound(); err != nil {
		return fmt.Errorf("failed to build outbound: %w", err)
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
	s.closeOnce.Do(func() {
		// 优先取消注册
		if s.registerID != "" && s.apiClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), service.DefaultTimeout)
			defer cancel()
			if err := s.apiClient.Unregister(ctx, api.AnyTLS, s.registerID); err != nil {
				logrus.WithError(err).Error("failed to unregister node")
			} else {
				logrus.WithField("register_id", s.registerID).Info("Node unregistered successfully")
				// 取消注册成功后，清空状态文件
				if clearErr := ClearState(s.dataDir); clearErr != nil {
					logrus.WithError(clearErr).Error("failed to clear state after unregister")
				}
			}
		}
		// 取消上下文，停止新的处理
		s.cancel()
		// 关闭所有监听器，忽略重复关闭错误
		for _, ln := range s.listeners {
			if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.closeErr = fmt.Errorf("failed to close listener: %w", err)
			}
		}
		// 关闭用户服务（判空以适配启动早期失败场景）
		if s.userService != nil {
			if err := s.userService.Close(); err != nil && s.closeErr == nil {
				s.closeErr = fmt.Errorf("failed to close user service: %w", err)
			}
		}
		logrus.Info("server stopped")
	})
	return s.closeErr
}
