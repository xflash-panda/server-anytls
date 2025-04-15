package server

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"runtime/debug"
	"strings"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/session"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sirupsen/logrus"
)

func handleTcpConnection(ctx context.Context, c net.Conn, s *Server) {
	defer func() {

		if r := recover(); r != nil {

			logrus.Errorln("[BUG]", r, string(debug.Stack()))
		}
	}()

	c = tls.Server(c, s.tlsConfig)
	defer c.Close()

	b := buf.NewPacket()
	defer b.Release()

	n, err := b.ReadOnceFrom(c)
	if err != nil {

		logrus.Debugln("ReadOnceFrom:", err)
		return
	}

	c = bufio.NewCachedConn(c, b)

	by, err := b.ReadBytes(32)
	if err != nil || !s.userService.Auth(by) {

		b.Resize(0, n)
		return
	}

	paddingLen := binary.BigEndian.Uint16(by)
	if paddingLen > 0 {
		_, err = b.ReadBytes(int(paddingLen))
		if err != nil {
			b.Resize(0, n)
			return
		}
	}

	session := session.NewServerSession(c, func(stream *session.Stream) {

		defer func() {

			if r := recover(); r != nil {

				logrus.Errorln("[BUG]", r, string(debug.Stack()))
			}
		}()
		defer stream.Close()

		destination, err := M.SocksaddrSerializer.ReadAddrPort(stream)
		if err != nil {
			logrus.Debugln("ReadAddrPort:", err)
			return
		}

		if strings.Contains(destination.String(), "udp-over-tcp.arpa") {

			proxyOutboundUoT(ctx, stream, destination)
		} else {

			proxyOutboundTCP(ctx, stream, destination)
		}
	}, &padding.DefaultPaddingFactory)
	session.Run()
}
