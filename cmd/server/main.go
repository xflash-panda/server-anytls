package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/xflash-panda/server-anytls/internal/app/server"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
	api "github.com/xflash-panda/server-client/pkg"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/xflash-panda/acl-engine/pkg/acl"
)

const (
	Name      = "anytls-node"
	Version   = "0.2.21"
	CopyRight = "XFLASH-PANDA@2021"
)

func main() {
	var apiConfig api.Config
	var serviceConfig service.Config
	var certConfig server.CertConfig
	var logLevel string
	var aclConfPath string
	var dataDir string
	var refreshGeoData bool

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
				Name:        "acl_conf_file",
				Usage:       "ACL config file for ACL and Outbounds (.yaml format)",
				EnvVars:     []string{"X_PANDA_ANYTLS_ACL_CONF_FILE", "ACL_CONF_FILE"},
				Required:    false,
				Destination: &aclConfPath,
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
				Name:        "heartbeat_interval, hi",
				Usage:       "API request cycle(heartbeat), unit: second",
				EnvVars:     []string{"X_PANDA_ANYTLS_HEARTBEAT_INTERVAL", "HEARTBEAT_INTERVAL"},
				Value:       time.Second * 180,
				DefaultText: "180",
				Required:    false,
				Destination: &serviceConfig.HeartbeatInterval,
			},
			&cli.StringFlag{
				Name:        "log_mode",
				Value:       server.LogLevelError,
				Usage:       "Log mode",
				EnvVars:     []string{"X_PANDA_ANYTLS_LOG_LEVEL", "LOG_LEVEL"},
				Destination: &logLevel,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "data_dir",
				Usage:       "Data directory for persisting state and other data",
				EnvVars:     []string{"X_PANDA_ANYTLS_DATA_DIR", "DATA_DIR"},
				Value:       "/var/lib/anytls-node",
				DefaultText: "/var/lib/anytls-node",
				Required:    false,
				Destination: &dataDir,
			},
			&cli.BoolFlag{
				Name:        "refresh_geodata",
				Usage:       "force refresh geoip and geosite databases on startup",
				EnvVars:     []string{"X_PANDA_ANYTLS_REFRESH_GEODATA", "REFRESH_GEODATA"},
				Destination: &refreshGeoData,
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

			// Force refresh geodata if requested
			if refreshGeoData {
				if err := forceRefreshGeoData(dataDir); err != nil {
					return fmt.Errorf("failed to refresh geodata: %w", err)
				}
			}

			srv, err = server.New(apiConfig, serviceConfig, certConfig, aclConfPath, dataDir)
			if err != nil {
				log.WithError(err).Error("failed to init server")
				return err
			}

			shutdown := func() {
				log.Infoln("shutting down...")
				if srv != nil {
					if err := srv.Close(); err != nil {
						log.WithError(err).Errorln("shutdown error")
					}
				}
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

func forceRefreshGeoData(dataDir string) error {
	if dataDir == "" {
		dataDir = server.DefaultDataDir
	}

	// Ensure directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Remove existing files to force re-download
	geoIPPath := filepath.Join(dataDir, acl.DefaultGeoIPFilename(acl.GeoIPFormatMMDB))
	geoSitePath := filepath.Join(dataDir, acl.DefaultGeoSiteFilename(acl.GeoSiteFormatSing))

	_ = os.Remove(geoIPPath)
	_ = os.Remove(geoSitePath)

	loader := &acl.AutoGeoLoader{
		DataDir:       dataDir,
		GeoIPFormat:   acl.GeoIPFormatMMDB,
		GeoSiteFormat: acl.GeoSiteFormatSing,
		GeoIPURL:      acl.MetaCubeXGeoIPMMDBURL,
		GeoSiteURL:    acl.MetaCubeXGeoSiteDBURL,
		Logger: func(format string, args ...interface{}) {
			log.Infof(format, args...)
		},
	}

	log.Info("Refreshing geoip database...")
	if _, err := loader.LoadGeoIP(); err != nil {
		return fmt.Errorf("failed to refresh geoip: %w", err)
	}
	log.Info("geoip database refreshed")

	log.Info("Refreshing geosite database...")
	if _, err := loader.LoadGeoSite(); err != nil {
		return fmt.Errorf("failed to refresh geosite: %w", err)
	}
	log.Info("geosite database refreshed")

	return nil
}
