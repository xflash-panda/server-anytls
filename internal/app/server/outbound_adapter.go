package server

import (
	"net"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/xflash-panda/acl-engine/pkg/outbound"
	"github.com/xflash-panda/acl-engine/pkg/router"
)

// OutboundAdapter wraps acl-engine's router.Router to provide
// a hysteria-compatible Outbound interface.
// It implements the interface expected by outbound_tcp.go functions.
type OutboundAdapter struct {
	router *router.Router
}

// NewOutboundAdapter creates a new OutboundAdapter.
func NewOutboundAdapter(r *router.Router) *OutboundAdapter {
	return &OutboundAdapter{router: r}
}

// TCP establishes a TCP connection to the given address string.
// addr format: "host:port"
func (a *OutboundAdapter) TCP(addr string) (net.Conn, error) {
	log.WithField("addr", addr).Debug("outbound TCP dial")
	oa, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	conn, err := a.router.DialTCP(oa)
	if err != nil {
		log.WithFields(log.Fields{"addr": addr, "error": err}).Debug("outbound TCP dial failed")
	} else {
		log.WithField("addr", addr).Debug("outbound TCP dial success")
	}
	return conn, err
}

// UDP creates a UDP connection for the given address string.
// addr format: "host:port"
func (a *OutboundAdapter) UDP(addr string) (UDPConn, error) {
	oa, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	uc, err := a.router.DialUDP(oa)
	if err != nil {
		return nil, err
	}
	return &udpConnWrapper{uc}, nil
}

// UDPConn is the hysteria-compatible UDP connection interface.
type UDPConn interface {
	ReadFrom(p []byte) (n int, addr string, err error)
	WriteTo(p []byte, addr string) (n int, err error)
	Close() error
}

// udpConnWrapper wraps acl-engine's UDPConn to provide hysteria-compatible interface.
type udpConnWrapper struct {
	conn outbound.UDPConn
}

func (w *udpConnWrapper) ReadFrom(p []byte) (n int, addr string, err error) {
	n, oa, err := w.conn.ReadFrom(p)
	if err != nil {
		return 0, "", err
	}
	if oa != nil {
		addr = oa.String()
	}
	return n, addr, nil
}

func (w *udpConnWrapper) WriteTo(p []byte, addr string) (n int, err error) {
	oa, err := parseAddr(addr)
	if err != nil {
		return 0, err
	}
	return w.conn.WriteTo(p, oa)
}

func (w *udpConnWrapper) Close() error {
	return w.conn.Close()
}

// parseAddr parses "host:port" string into *outbound.Addr.
func parseAddr(addr string) (*outbound.Addr, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, err
	}
	return &outbound.Addr{
		Host: host,
		Port: uint16(port),
	}, nil
}
