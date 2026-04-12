package server

import (
	"crypto/tls"
	"net"
	"net/netip"
	"time"

	pb "github.com/xflash-panda/server-agent-proto/pkg"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"
)

const (
	LogLevelDebug = "debug"
	LogLevelError = "error"
	LogLevelInfo  = "info"
)

type CertConfig struct {
	CertFile string
	KeyFile  string
}

func (c *CertConfig) Load() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}

type ExtConfig struct {
	Outbounds []serverConfigOutboundEntry `mapstructure:"outbounds"`
	ACL       serverConfigACL             `mapstructure:"acl"`
	Sniff     serverConfigSniff           `mapstructure:"sniff"`
}

type serverConfigACL struct {
	Inline []string `mapstructure:"inline"`
}

type serverConfigOutboundEntry struct {
	Name   string                     `mapstructure:"name"`
	Type   string                     `mapstructure:"type"`
	Direct serverConfigOutboundDirect `mapstructure:"direct"`
	SOCKS5 serverConfigOutboundSOCKS5 `mapstructure:"socks5"`
	HTTP   serverConfigOutboundHTTP   `mapstructure:"http"`
}

type serverConfigSniff struct {
	Enable        bool          `mapstructure:"enable"`
	Timeout       time.Duration `mapstructure:"timeout"`
	RewriteDomain bool          `mapstructure:"rewriteDomain"`
	TCPPorts      string        `mapstructure:"tcpPorts"`
	UDPPorts      string        `mapstructure:"udpPorts"`
}

type serverConfigOutboundDirect struct {
	Mode       string `mapstructure:"mode"`
	BindIPv4   string `mapstructure:"bindIPv4"`
	BindIPv6   string `mapstructure:"bindIPv6"`
	BindDevice string `mapstructure:"bindDevice"`
}

type serverConfigOutboundSOCKS5 struct {
	Addr     string `mapstructure:"addr"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type serverConfigOutboundHTTP struct {
	URL      string `mapstructure:"url"`
	Insecure bool   `mapstructure:"insecure"`
}

// Options contains inputs required for Server to initialize its
// configuration and dependencies during Start.
type Options struct {
	AgentClient   pb.AgentClient
	ServiceConfig *service.Config
	CertConfig    CertConfig
	ACLConfPath   string
	DataDir       string
}

// func geoDownloadFunc(filename, url string) {
// 	log.Info("downloading database", zap.String("filename", filename), zap.String("url", url))
// }

// func geoDownloadErrFunc(err error) {
// 	if err != nil {
// 		log.Error("failed to download database", zap.Error(err))
// 	}
// }

// udpConnAdapter adapts our UDPConn interface to net.PacketConn.
type udpConnAdapter struct {
	UDPConn // from outbound_adapter.go
}

func (a *udpConnAdapter) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addrStr, err := a.UDPConn.ReadFrom(p)
	if err != nil {
		return 0, nil, err
	}
	ap, err := netip.ParseAddrPort(addrStr)
	if err != nil {
		return 0, nil, err
	}
	return n, net.UDPAddrFromAddrPort(ap), nil
}

func (a *udpConnAdapter) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return a.UDPConn.WriteTo(p, addr.String())
}

func (a *udpConnAdapter) LocalAddr() net.Addr {
	return nil
}

func (a *udpConnAdapter) SetDeadline(t time.Time) error {
	return nil
}

func (a *udpConnAdapter) SetReadDeadline(t time.Time) error {
	return nil
}

func (a *udpConnAdapter) SetWriteDeadline(t time.Time) error {
	return nil
}
