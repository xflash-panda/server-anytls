package server

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"net"
	"runtime/debug"
	"strings"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/session"
	"github.com/xflash-panda/server-anytls/internal/pkg/service"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sirupsen/logrus"
)

// contextKey 用于在 Context 中存储 userService
type contextKey struct{}

// WithUserService 将 userService 注入到 Context 中
func WithUserService(ctx context.Context, userService *service.UsersService) context.Context {
	return context.WithValue(ctx, contextKey{}, userService)
}

// GetUserService 从 Context 中获取 userService
func GetUserService(ctx context.Context) *service.UsersService {
	if userService, ok := ctx.Value(contextKey{}).(*service.UsersService); ok {
		return userService
	}
	return nil
}

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
	defer c.Close()
	b := buf.NewPacket()
	defer b.Release()
	n, err := b.ReadOnceFrom(c)
	if err != nil {
		logrus.WithError(err).Debug("failed to read initial data")
		return
	}
	c = bufio.NewCachedConn(c, b)
	passwordBytes, err := b.ReadBytes(32)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"remote_addr": c.RemoteAddr().String(),
			"error":       err,
		}).Error("failed to read password")
		b.Resize(0, n)
		return
	}
	passwordHexString := hex.EncodeToString(passwordBytes)
	if !s.userService.Auth(passwordHexString) {
		logrus.Debug("authentication failed")
		b.Resize(0, n)
		return
	} else {
		logrus.Debugln("authentication success")
	}

	paddingLenBytes, err := b.ReadBytes(2)
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
	userId, ok := s.userService.GetUserId(passwordBytes)
	if !ok {
		logrus.Debug("failed to get user ID")
		return
	}
	logrus.Debugln("user ID:", userId, "password:", hex.EncodeToString(passwordBytes))
	ctx = WithUserService(ctx, s.userService)
	countedConn := &CountedConn{
		Conn:              c,
		userId:            userId,
		ctx:               ctx,
		passwordHexString: passwordHexString,
	}
	session := session.NewServerSession(countedConn, func(stream *session.Stream) {
		logrus.Debugln("stream created")
		defer func() {
			if r := recover(); r != nil {
				logrus.WithFields(logrus.Fields{
					"error": r,
					"stack": string(debug.Stack()),
				}).Error("[BUG] stream panic recovered")
			}
		}()
		defer stream.Close()

		// 验证Auth, 从conn中获取passwordHexString
		passwordHexString := stream.GetConn().(*CountedConn).passwordHexString
		if !s.userService.Auth(passwordHexString) {
			logrus.Debug("authentication failed on stream, origin password:", passwordHexString)
			return
		}

		// 更新流量请求次数
		s.userService.UpdateTraffic(userId, 0, 0, 1)

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
	s.userService.UpdateTraffic(userId, uint64(n), 0, 0)
	session.Run()
}

// CountedConn 包装原始连接以统计流量
type CountedConn struct {
	net.Conn
	userId            int
	ctx               context.Context
	passwordHexString string
}

// Read 实现了 net.Conn 接口，统计下行流量
func (c *CountedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		if userService := GetUserService(c.ctx); userService != nil {
			userService.UpdateTraffic(c.userId, uint64(n), 0, 0)
		}
	}
	return
}

// Write 实现了 net.Conn 接口，统计上行流量
func (c *CountedConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if n > 0 {
		if userService := GetUserService(c.ctx); userService != nil {
			userService.UpdateTraffic(c.userId, 0, uint64(n), 0)
		}
	}
	return
}
