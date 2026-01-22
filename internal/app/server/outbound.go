package server

import (
	"errors"
	"strings"

	"github.com/xflash-panda/acl-engine/pkg/acl"
	"github.com/xflash-panda/acl-engine/pkg/outbound"
	"github.com/xflash-panda/acl-engine/pkg/router"
)

// buildOutbound constructs the outbound and ACL engine based on extConfig and
// assigns the result to s.outbound.
func (s *Server) buildOutbound() error {
	var obs []router.OutboundEntry

	if s.extConfig != nil && len(s.extConfig.Outbounds) > 0 {
		obs = make([]router.OutboundEntry, len(s.extConfig.Outbounds))
		for i, entry := range s.extConfig.Outbounds {
			if entry.Name == "" {
				return configError{Field: "outbounds.name", Err: errors.New("empty outbound name")}
			}
			var ob outbound.Outbound
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
			obs[i] = router.OutboundEntry{Name: entry.Name, Outbound: ob}
		}
	}

	// If no outbounds configured, use default direct
	if len(obs) == 0 {
		obs = []router.OutboundEntry{{
			Name:     "default",
			Outbound: outbound.NewDirect(outbound.DirectModeAuto),
		}}
	}

	// Create GeoLoader with mmdb/db format
	geoLoader := &acl.AutoGeoLoader{
		DataDir:       s.opts.DataDir,
		GeoIPFormat:   acl.GeoIPFormatMMDB,
		GeoSiteFormat: acl.GeoSiteFormatSing,
		GeoIPURL:      acl.MetaCubeXGeoIPMMDBURL,
		GeoSiteURL:    acl.MetaCubeXGeoSiteDBURL,
		Logger: func(format string, args ...interface{}) {
			geoDownloadFunc(format, "")
		},
	}

	// Build router with ACL rules or use first outbound directly
	if s.extConfig != nil && len(s.extConfig.ACL.Inline) > 0 {
		r, err := router.New(
			strings.Join(s.extConfig.ACL.Inline, "\n"),
			obs,
			geoLoader,
		)
		if err != nil {
			return configError{Field: "acl.inline", Err: err}
		}
		s.outbound = NewOutboundAdapter(r)
	} else {
		// No ACL rules, wrap first outbound directly
		r, err := router.New("default(all)", obs, geoLoader)
		if err != nil {
			return configError{Field: "outbound", Err: err}
		}
		s.outbound = NewOutboundAdapter(r)
	}

	return nil
}
