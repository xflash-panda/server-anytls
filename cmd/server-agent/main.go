package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	pb "github.com/xflash-panda/server-agent-proto/pkg"
	"github.com/xflash-panda/server-anytls/internal/app/server"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	Name      = "anytls-agent-node"
	Version   = "0.0.2"
	CopyRight = "XFLASH-PANDA@2021"
)

func init() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "print only the version",
	}
	cli.ErrWriter = io.Discard

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("version=%s\n", Version)
	}
}

func main() {
	var agentHost string
	var agentPort int
	var serviceConfig service.Config
	var certConfig server.CertConfig
	var logLevel string

	app := &cli.App{
		Name:      Name,
		Version:   Version,
		Copyright: CopyRight,
		Usage:     "Provide anytls service for the v2Board(XFLASH-PANDA)",
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
				Name:        "cert_file",
				Usage:       "Cert file",
				EnvVars:     []string{"X_PANDA_ANYTLS_CERT_FILE", "CERT_FILE"},
				Value:       "/root/.cert/server.crt",
				Required:    false,
				DefaultText: "/root/.cert/server.crt",
				Destination: &certConfig.CertFile,
			},
			&cli.StringFlag{
				Name:        "key_file",
				Usage:       "Key file",
				EnvVars:     []string{"X_PANDA_ANYTLS_KEY_FILE", "KEY_FILE"},
				Value:       "/root/.cert/server.key",
				Required:    false,
				DefaultText: "/root/.cert/server.key",
				Destination: &certConfig.KeyFile,
			},
			&cli.IntFlag{
				Name:        "node",
				Usage:       "Node ID",
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
				Usage:       "Log mode",
				EnvVars:     []string{"X_PANDA_ANYTLS_LOG_LEVEL", "LOG_LEVEL"},
				Destination: &logLevel,
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
			if logLevel != server.LogLevelDebug {
				defer func() {
					if r := recover(); r != nil {
						log.WithField("panic", r).Error("Application panic recovered")
						panic(r)
					}
				}()
			}

			agentAddr := fmt.Sprintf("%s:%d", agentHost, agentPort)
			agentConn, err := grpc.Dial(agentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithKeepaliveParams(
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

			ctx, cancel := context.WithTimeout(context.Background(), service.DefaultTimeout)
			defer cancel()

			r, err := agentClient.Config(ctx, &pb.ConfigRequest{Params: &pb.CommonParams{NodeId: int32(serviceConfig.NodeID), NodeType: pb.NodeType_ANYTLS}})
			if err != nil {
				return err
			}

			anytlsConfig, err := api.UnmarshalAnyTLSConfig(r.GetRawData())
			if err != nil {
				return err
			}

			tlsConfig, err := certConfig.Load()
			if err != nil {
				log.WithError(err).Error("Failed to load TLS configuration")
				return fmt.Errorf("failed to load TLS config: %w", err)
			}

			userService := service.NewUsersService(&serviceConfig, agentClient)
			srv, err := server.New(anytlsConfig, userService, tlsConfig)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("Failed to create server")
				return fmt.Errorf("failed to create server: %w", err)
			}

			log.Infoln("Starting server...")

			// 创建一个用于等待服务器关闭的通道
			serverDone := make(chan error, 1)
			go func() {
				serverDone <- srv.Run()
			}()

			// 等待信号
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

			select {
			case <-quit:
				log.Info("Shutting down server...")
				if err := srv.Stop(); err != nil {
					log.WithError(err).Error("Error stopping server")
				}
			case err := <-serverDone:
				if err != nil {
					log.WithError(err).Error("Server stopped with error")
					return err
				}
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Fatal("Application failed to start")
	}
}
