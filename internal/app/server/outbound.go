package server

import (
	"errors"
	"strings"

	C "github.com/apernet/hysteria/core/v2/server"
	"github.com/apernet/hysteria/extras/v2/outbounds"
)

// buildOutbound constructs the outbound and ACL engine based on extConfig and
// assigns the result to s.outbound.
func (s *Server) buildOutbound() error {
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
	// Maintain type compatibility with C.Outbound
	var _ C.Outbound = s.outbound
	return nil
}
