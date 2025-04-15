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

	// 设置 TLS
	c = tls.Server(c, s.tlsConfig)
	defer c.Close()

	// 读取初始数据
	b := buf.NewPacket()
	defer b.Release()

	n, err := b.ReadOnceFrom(c)
	if err != nil {
		logrus.WithError(err).Debug("failed to read initial data")
		return
	}

	c = bufio.NewCachedConn(c, b)

	// 验证密码
	passwordBytes, err := b.ReadBytes(32)
	if err != nil || !s.userService.Auth(passwordBytes) {
		logrus.Debug("authentication failed")
		b.Resize(0, n)
		return
	}

	// 读取填充长度
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

	// 获取用户ID
	userId, ok := s.userService.GetUserId(passwordBytes)
	if !ok {
		logrus.Debug("failed to get user ID")
		return
	}

	// 将 userService 注入到 Context 中
	ctx = WithUserService(ctx, s.userService)

	// 包装连接以统计流量
	countedConn := &CountedConn{
		Conn:   c,
		userId: userId,
		ctx:    ctx,
	}

	// 创建会话
	session := session.NewServerSession(countedConn, func(stream *session.Stream) {
		defer func() {
			if r := recover(); r != nil {
				logrus.WithFields(logrus.Fields{
					"error": r,
					"stack": string(debug.Stack()),
				}).Error("[BUG] stream panic recovered")
			}
		}()
		defer stream.Close()

		// 读取目标地址
		destination, err := M.SocksaddrSerializer.ReadAddrPort(stream)
		if err != nil {
			logrus.WithError(err).Debug("failed to read destination address")
			return
		}

		// 根据目标地址选择代理方式
		if strings.Contains(destination.String(), "udp-over-tcp.arpa") {
			proxyOutboundUoT(ctx, stream, destination)
		} else {
			proxyOutboundTCP(ctx, stream, destination)
		}
	}, &padding.DefaultPaddingFactory)

	// 初始连接数据的上行流量统计
	s.userService.UpdateTraffic(userId, uint64(n), 0)

	// 运行会话
	session.Run()
}

// CountedConn 包装原始连接以统计流量
type CountedConn struct {
	net.Conn
	userId int
	ctx    context.Context
}

// Read 实现了 net.Conn 接口，统计下行流量
func (c *CountedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		if userService := GetUserService(c.ctx); userService != nil {
			userService.UpdateTraffic(c.userId, 0, uint64(n))
		}
	}
	return
}

// Write 实现了 net.Conn 接口，统计上行流量
func (c *CountedConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if n > 0 {
		if userService := GetUserService(c.ctx); userService != nil {
			userService.UpdateTraffic(c.userId, uint64(n), 0)
		}
	}
	return
}
