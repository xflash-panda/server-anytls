package server

import (
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"time"

	C "github.com/apernet/hysteria/core/v2/server"
	"github.com/apernet/hysteria/extras/v2/outbounds"
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

func serverConfigOutboundDirectToOutbound(c serverConfigOutboundDirect) (outbounds.PluggableOutbound, error) {
	var mode outbounds.DirectOutboundMode
	switch strings.ToLower(c.Mode) {
	case "", "auto":
		mode = outbounds.DirectOutboundModeAuto
	case "64":
		mode = outbounds.DirectOutboundMode64
	case "46":
		mode = outbounds.DirectOutboundMode46
	case "6":
		mode = outbounds.DirectOutboundMode6
	case "4":
		mode = outbounds.DirectOutboundMode4
	default:
		return nil, configError{Field: "outbounds.direct.mode", Err: errors.New("unsupported mode")}
	}
	bindIP := len(c.BindIPv4) > 0 || len(c.BindIPv6) > 0
	bindDevice := len(c.BindDevice) > 0
	if bindIP && bindDevice {
		return nil, configError{Field: "outbounds.direct", Err: errors.New("cannot bind both IP and device")}
	}
	if bindIP {
		ip4, ip6 := net.ParseIP(c.BindIPv4), net.ParseIP(c.BindIPv6)
		if len(c.BindIPv4) > 0 && ip4 == nil {
			return nil, configError{Field: "outbounds.direct.bindIPv4", Err: errors.New("invalid IPv4 address")}
		}
		if len(c.BindIPv6) > 0 && ip6 == nil {
			return nil, configError{Field: "outbounds.direct.bindIPv6", Err: errors.New("invalid IPv6 address")}
		}
		return outbounds.NewDirectOutboundBindToIPs(mode, ip4, ip6)
	}
	if bindDevice {
		return outbounds.NewDirectOutboundBindToDevice(mode, c.BindDevice)
	}
	return outbounds.NewDirectOutboundSimple(mode), nil
}

func serverConfigOutboundSOCKS5ToOutbound(c serverConfigOutboundSOCKS5) (outbounds.PluggableOutbound, error) {
	if c.Addr == "" {
		return nil, configError{Field: "outbounds.socks5.addr", Err: errors.New("empty socks5 address")}
	}
	return outbounds.NewSOCKS5Outbound(c.Addr, c.Username, c.Password), nil
}

func serverConfigOutboundHTTPToOutbound(c serverConfigOutboundHTTP) (outbounds.PluggableOutbound, error) {
	if c.URL == "" {
		return nil, configError{Field: "outbounds.http.url", Err: errors.New("empty http address")}
	}
	return outbounds.NewHTTPOutbound(c.URL, c.Insecure)
}

// func geoDownloadFunc(filename, url string) {
// 	log.Info("downloading database", zap.String("filename", filename), zap.String("url", url))
// }

// func geoDownloadErrFunc(err error) {
// 	if err != nil {
// 		log.Error("failed to download database", zap.Error(err))
// 	}
// }

type udpConnAdapter struct {
	C.UDPConn
}

func (a *udpConnAdapter) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addrStr, err := a.UDPConn.ReadFrom(p)
	if err != nil {
		return 0, nil, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return 0, nil, err
	}
	return n, udpAddr, nil
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
