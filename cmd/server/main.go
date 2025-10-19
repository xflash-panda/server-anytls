package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xflash-panda/server-anytls/internal/app/server"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"
)

const (
	Name      = "anytls-node"
	Version   = "0.0.9"
	CopyRight = "XFLASH-PANDA@2021"
)

func main() {
	var apiConfig api.Config
	var serviceConfig service.Config
	var certConfig server.CertConfig
	var logLevel string
	var extConfPath string

	app := &cli.App{
		Name:      Name,
		Version:   Version,
		Copyright: CopyRight,
		Usage:     "Provide anytls service for the v2Board(XFLASH-PANDA)",
		ErrWriter: io.Discard,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "api",
				Usage:       "Server address",
				EnvVars:     []string{"X_PANDA_ANYTLS_API", "API"},
				Required:    true,
				Destination: &apiConfig.APIHost,
			},
			&cli.StringFlag{
				Name:        "token",
				Usage:       "Token of server API",
				EnvVars:     []string{"X_PANDA_ANYTLS_TOKEN", "TOKEN"},
				Required:    true,
				Destination: &apiConfig.Token,
			},
			&cli.StringFlag{
				Name:        "ext_conf_file",
				Usage:       "Extended profiles for ACL and Outbounds(.yaml format)",
				EnvVars:     []string{"X_PANDA_ANYTLS_EXT_CONF_FILE", "EXT_CONF_FILE"},
				Required:    false,
				Destination: &extConfPath,
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

			apiClient := api.New(&apiConfig)

			nodeConf, err := apiClient.Config(api.NodeId(serviceConfig.NodeID), api.AnyTLS)
			if err != nil {
				log.WithFields(log.Fields{
					"node_id": serviceConfig.NodeID,
					"error":   err.Error(),
				}).Error("Failed to get node configuration")
				return fmt.Errorf("failed to get node config: %w", err)
			}

			log.WithFields(log.Fields{
				"node_id":   serviceConfig.NodeID,
				"node_info": nodeConf.String(),
			}).Info("Server configuration loaded successfully")

			tlsConfig, err := certConfig.Load()
			if err != nil {
				log.WithError(err).Error("Failed to load TLS configuration")
				return fmt.Errorf("failed to load TLS config: %w", err)
			}

			var extConfig *server.ExtConfig
			if extConfPath != "" {
				viper.SetConfigFile(extConfPath)
				if err := viper.ReadInConfig(); err != nil {
					return fmt.Errorf("failed to read ext config: %w", err)
				}

				if err := viper.Unmarshal(&extConfig); err != nil {
					return fmt.Errorf("failed to unmarshal ext config: %w", err)
				}
			}

			userService := service.NewUsersService(&serviceConfig, apiClient)
			srv, err := server.New(nodeConf, userService, tlsConfig, extConfig)
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
				serverDone <- srv.Start()
			}()

			// 等待信号
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

			select {
			case <-quit:
				log.Info("Shutting down server...")
				if err := srv.Close(); err != nil {
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
