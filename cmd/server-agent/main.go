package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	pb "github.com/xflash-panda/server-agent-proto/pkg"
	"github.com/xflash-panda/server-anytls/internal/app/server"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	Name      = "anytls-agent-node"
	Version   = "0.1.4"
	CopyRight = "XFLASH-PANDA@2021"
)

func main() {
	var agentHost string
	var agentPort int
	var serviceConfig service.Config
	var certConfig server.CertConfig
	var logLevel string
	var extConfPath string
	var dataDir string

	app := &cli.App{
		Name:      Name,
		Version:   Version,
		Copyright: CopyRight,
		Usage:     "Provide anytls service for the v2Board(XFLASH-PANDA)",
		ErrWriter: io.Discard,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server_host, sh",
				Value:       "127.0.0.1",
				Usage:       "server host(agent)",
				EnvVars:     []string{"X_PANDA_ANYTLS_SERVER_AGENT_HOST", "SERVER_HOST"},
				Destination: &agentHost,
			},
			&cli.IntFlag{
				Name:        "port, p",
				Value:       8082,
				Usage:       "server port(agent)",
				EnvVars:     []string{"X_PANDA_ANYTLS_SERVER_AGENT_PORT", "SERVER_PORT"},
				Destination: &agentPort,
			},
			&cli.StringFlag{
				Name:        "ext_conf_file",
				Usage:       "extended profiles for ACL and Outbounds(.yaml format)",
				EnvVars:     []string{"X_PANDA_ANYTLS_EXT_CONF_FILE", "EXT_CONF_FILE"},
				Required:    false,
				Destination: &extConfPath,
			},
			&cli.StringFlag{
				Name:        "cert_file",
				Usage:       "cert file",
				EnvVars:     []string{"X_PANDA_ANYTLS_CERT_FILE", "CERT_FILE"},
				Value:       "/root/.cert/server.crt",
				Required:    false,
				DefaultText: "/root/.cert/server.crt",
				Destination: &certConfig.CertFile,
			},
			&cli.StringFlag{
				Name:        "key_file",
				Usage:       "key file",
				EnvVars:     []string{"X_PANDA_ANYTLS_KEY_FILE", "KEY_FILE"},
				Value:       "/root/.cert/server.key",
				Required:    false,
				DefaultText: "/root/.cert/server.key",
				Destination: &certConfig.KeyFile,
			},
			&cli.IntFlag{
				Name:        "node",
				Usage:       "node ID",
				EnvVars:     []string{"X_PANDA_ANYTLS_NODE", "NODE"},
				Required:    true,
				Destination: &serviceConfig.NodeID,
			},
			&cli.DurationFlag{
				Name:        "fetch_users_interval, fui",
				Usage:       "API request cycle(fetch users), unit: second",
				EnvVars:     []string{"X_PANDA_ANYTLS_FETCH_USER_INTERVAL", "FETCH_USER_INTERVAL"},
				Value:       time.Second * 60,
				DefaultText: "60",
				Required:    false,
				Destination: &serviceConfig.FetchUserInterval,
			},
			&cli.DurationFlag{
				Name:        "report_traffics_interval, fui",
				Usage:       "API request cycle(report traffics), unit: second",
				EnvVars:     []string{"X_PANDA_ANYTLS_FETCH_USER_INTERVAL", "REPORT_TRAFFICS_INTERVAL"},
				Value:       time.Second * 80,
				DefaultText: "80",
				Required:    false,
				Destination: &serviceConfig.ReportTrafficInterval,
			},
			&cli.DurationFlag{
				Name:        "heartbeat_interval",
				Usage:       "API request cycle(heartbeat), unit: second",
				EnvVars:     []string{"X_PANDA_ANYTLS_HEARTBEAT_INTERVAL", "HEARTTBEAT_INTERVAL"},
				Value:       time.Second * 60,
				DefaultText: "60 seconds",
				Required:    false,
				Destination: &serviceConfig.HeartBeatInterval,
			},
			&cli.StringFlag{
				Name:        "log_mode",
				Value:       server.LogLevelError,
				Usage:       "log mode",
				EnvVars:     []string{"X_PANDA_ANYTLS_LOG_LEVEL", "LOG_LEVEL"},
				Destination: &logLevel,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "data_dir",
				Value:       server.DefaultDataDir,
				Usage:       "data directory for storing state",
				EnvVars:     []string{"X_PANDA_ANYTLS_DATA_DIR", "DATA_DIR"},
				Destination: &dataDir,
				Required:    false,
			},
		},
		Before: func(c *cli.Context) error {
			log.SetFormatter(&log.TextFormatter{
				TimestampFormat: time.RFC3339,
				FullTimestamp:   true,
			})

			switch logLevel {
			case server.LogLevelDebug:
				log.SetLevel(log.DebugLevel)
				log.SetReportCaller(true)
			case server.LogLevelInfo:
				log.SetLevel(log.InfoLevel)
			case server.LogLevelError:
				log.SetLevel(log.ErrorLevel)
			default:
				return fmt.Errorf("unsupported log level: %s", logLevel)
			}

			return nil
		},
		Action: func(c *cli.Context) error {
			var srv *server.Server
			var err error
			agentAddr := fmt.Sprintf("%s:%d", agentHost, agentPort)
			agentConn, err := grpc.NewClient(agentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(
				keepalive.ClientParameters{
					Time:                30 * time.Second, // 每30秒发送一次keepalive探测
					Timeout:             10 * time.Second, // 如果10秒内没有响应，则认为连接断开
					PermitWithoutStream: true,             // 允许即使没有活动流的情况下也发送探测
				}))
			if err != nil {
				return fmt.Errorf("agent server connect error : %v", err)
			}
			agentClient := pb.NewAgentClient(agentConn)
			defer agentConn.Close()

			opts := &server.Options{
				AgentClient:   agentClient,
				ServiceConfig: &serviceConfig,
				CertConfig:    certConfig,
				ExtConfPath:   extConfPath,
				DataDir:       dataDir,
			}
			srv, err = server.New(opts)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("Failed to create server")
				return fmt.Errorf("failed to create server: %w", err)
			}

			log.Infoln("Starting server...")

			var shutdownOnce sync.Once
			shutdown := func() {
				shutdownOnce.Do(func() {
					log.Infoln("shutting down...")
					if srv != nil {
						if err := srv.Close(); err != nil {
							log.WithError(err).Errorln("shutdown error")
						}
					}

					if agentConn != nil {
						if err := agentConn.Close(); err != nil {
							log.WithError(err).Errorln("agent connection close error")
						}
					}
				})
			}

			// 确保无论正常退出还是异常退出都会调用 Close
			defer func() {
				if e := recover(); e != nil {
					log.Errorf("panic: %v", e)
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					log.Errorf("stack trace:\n%s", buf[:n])
					shutdown()
					os.Exit(1)
				} else {
					shutdown()
				}
			}()

			osSignals := make(chan os.Signal, 1)
			signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-osSignals
				shutdown()
			}()

			log.Infoln("Starting server...")
			if err := srv.Start(); err != nil {
				log.WithError(err).Error("Server stopped with error")
				return err
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Fatal("Application failed to start")
	}
}
