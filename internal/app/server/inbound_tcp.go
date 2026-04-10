package server

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"net"
	"runtime/debug"
	"strings"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/session"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sirupsen/logrus"
)

const (
	passwordLen       = 32
	paddingLenFieldSz = 2
)

// serverKey 用于在 Context 中存储 Server 实例
type serverKey struct{}

// WithServer 将 Server 实例注入到 Context 中
func WithServer(ctx context.Context, server *Server) context.Context {
	return context.WithValue(ctx, serverKey{}, server)
}

func handleTcpConnection(ctx context.Context, c net.Conn, s *Server) {
	defer func() {
		if r := recover(); r != nil {
			logrus.WithFields(logrus.Fields{
				"error": r,
				"stack": string(debug.Stack()),
			}).Error("[BUG] connection panic recovered")
		}
	}()
	c = tls.Server(c, s.tlsConfig)
	defer func() { _ = c.Close() }()
	b := buf.NewPacket()
	defer b.Release()
	n, err := b.ReadOnceFrom(c)
	if err != nil {
		logrus.WithError(err).Debug("failed to read initial data")
		return
	}
	c = bufio.NewCachedConn(c, b)
	passwordBytes, err := b.ReadBytes(passwordLen)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"remote_addr": c.RemoteAddr().String(),
			"error":       err,
		}).Error("failed to read password")
		b.Resize(0, n)
		return
	}
	passwordHexString := hex.EncodeToString(passwordBytes)
	userId, ok := s.userService.AuthAndGetUserId(passwordHexString)
	if !ok {
		logrus.Debug("authentication failed")
		b.Resize(0, n)
		return
	}
	logrus.Debugln("authentication success")

	paddingLenBytes, err := b.ReadBytes(paddingLenFieldSz)
	if err != nil {
		logrus.WithError(err).Debug("failed to read padding length")
		b.Resize(0, n)
		return
	}
	paddingLen := binary.BigEndian.Uint16(paddingLenBytes)
	if paddingLen > 0 {
		_, err = b.ReadBytes(int(paddingLen))
		if err != nil {
			logrus.WithError(err).Debug("failed to read padding data")
			b.Resize(0, n)
			return
		}
	}
	logrus.Debugln("user ID:", userId, "password:", passwordHexString)
	trafficItem := s.userService.GetTrafficItem(userId)
	// Count protocol header bytes consumed from buffer (not seen by CountedConn)
	trafficItem.Up.Add(uint64(passwordLen + paddingLenFieldSz + paddingLen))
	countedConn := &CountedConn{
		Conn:        c,
		trafficItem: trafficItem,
	}
	sess := session.NewServerSession(countedConn, func(stream *session.Stream) {
		logrus.Debugln("stream created")
		defer func() {
			if r := recover(); r != nil {
				logrus.WithFields(logrus.Fields{
					"error": r,
					"stack": string(debug.Stack()),
				}).Error("[BUG] stream panic recovered")
			}
		}()
		defer func() { _ = stream.Close() }()

		// 更新流量请求次数
		trafficItem.Count.Add(1)

		destination, err := M.SocksaddrSerializer.ReadAddrPort(stream)
		if err != nil {
			logrus.WithError(err).Debug("failed to read destination address")
			return
		}
		if strings.Contains(destination.String(), "udp-over-tcp.arpa") {
			err = proxyOutboundUoTWithOutbound(ctx, stream, destination, s.outbound)
			if err != nil {
				logrus.WithError(err).Debug("failed to proxy outbound UoT")
			}
		} else {
			err = proxyOutboundTCPWithOutbound(ctx, stream, destination, s.outbound)
			if err != nil {
				logrus.WithError(err).Debug("failed to proxy outbound TCP")
			}

		}
	}, &padding.DefaultPaddingFactory)

	// Register connection for user
	s.userService.RegisterConnection(userId, sess)

	// Set cleanup hook to unregister connection when session closes
	sess.SetDieHook(func() {
		s.userService.UnregisterConnection(userId, sess)
	})

	sess.Run()
}

// CountedConn wraps a net.Conn with a cached TrafficItem for zero-lookup
// traffic accounting on every Read/Write.
type CountedConn struct {
	net.Conn
	trafficItem *service.TrafficItem
}

func (c *CountedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		c.trafficItem.Up.Add(uint64(n))
	}
	return n, err
}

func (c *CountedConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if n > 0 {
		c.trafficItem.Down.Add(uint64(n))
	}
	return n, err
}
