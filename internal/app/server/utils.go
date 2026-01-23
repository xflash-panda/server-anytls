package server

import (
	"errors"
	"net"
	"strings"

	"github.com/xflash-panda/acl-engine/pkg/outbound"
)

func serverConfigOutboundDirectToOutbound(c serverConfigOutboundDirect) (outbound.Outbound, error) {
	var mode outbound.DirectMode
	switch strings.ToLower(c.Mode) {
	case "", "auto":
		mode = outbound.DirectModeAuto
	case "64":
		mode = outbound.DirectMode64
	case "46":
		mode = outbound.DirectMode46
	case "6":
		mode = outbound.DirectMode6
	case "4":
		mode = outbound.DirectMode4
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
		return outbound.NewDirectBindToIPs(mode, ip4, ip6)
	}
	if bindDevice {
		return outbound.NewDirectBindToDevice(mode, c.BindDevice)
	}
	return outbound.NewDirect(mode), nil
}

func serverConfigOutboundSOCKS5ToOutbound(c serverConfigOutboundSOCKS5) (outbound.Outbound, error) {
	if c.Addr == "" {
		return nil, configError{Field: "outbounds.socks5.addr", Err: errors.New("empty socks5 address")}
	}
	return outbound.NewSOCKS5(c.Addr, c.Username, c.Password), nil
}

func serverConfigOutboundHTTPToOutbound(c serverConfigOutboundHTTP) (outbound.Outbound, error) {
	if c.URL == "" {
		return nil, configError{Field: "outbounds.http.url", Err: errors.New("empty http address")}
	}
	return outbound.NewHTTP(c.URL, c.Insecure)
}
