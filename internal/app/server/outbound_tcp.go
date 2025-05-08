package server // 声明包名为server

import ( // 导入所需的包
	"context" // 上下文包，用于控制超时、取消等
	"net"     // 网络包，提供网络相关的接口

	// 时间包，用于处理时间相关的操作
	C "github.com/apernet/hysteria/core/v2/server"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy" // 导入自定义proxy包

	"github.com/sagernet/sing/common/bufio"        // 导入bufio包，用于高效的缓冲IO
	E "github.com/sagernet/sing/common/exceptions" // 导入异常处理包，简写为E
	M "github.com/sagernet/sing/common/metadata"   // 导入元数据包，简写为M
	N "github.com/sagernet/sing/common/network"    // 导入网络工具包，简写为N
	"github.com/sagernet/sing/common/uot"          // 导入UoT协议包
	"github.com/sirupsen/logrus"                   // 导入日志库logrus
)

// 注释参数
// ctx context.Context, conn net.Conn, destination M.Socksaddr
//
// proxyOutboundTCP 处理TCP出站代理
func proxyOutboundTCP(ctx context.Context, conn net.Conn, destination M.Socksaddr) error {
	c, err := proxy.SystemDialer.DialContext(ctx, "tcp", destination.String()) // 使用系统拨号器发起到目标的TCP连接
	if err != nil {                                                            // 如果拨号失败
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err)) // 记录并报告握手失败
		return err                                               // 返回错误
	}

	err = N.ReportHandshakeSuccess(conn) // 报告握手成功
	if err != nil {                      // 如果报告失败
		return err // 返回错误
	}

	return bufio.CopyConn(ctx, conn, c) // 在入站连接和目标连接之间转发数据
}

// proxyOutboundUoT 处理UDP over TCP (UoT)出站代理
func proxyOutboundUoT(ctx context.Context, conn net.Conn, destination M.Socksaddr) error {

	request, err := uot.ReadRequest(conn) // 从入站连接读取UoT请求
	if err != nil {                       // 如果读取失败
		logrus.Debugln("proxyOutboundUoT ReadRequest:", err) // 打印调试日志
		return err                                           // 返回错误
	}

	c, err := net.ListenPacket("udp", "") // 创建一个UDP监听端口
	if err != nil {                       // 如果创建失败
		logrus.Debugln("proxyOutboundUoT ListenPacket:", err)    // 打印调试日志
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err)) // 记录并报告握手失败
		return err                                               // 返回错误
	}

	err = N.ReportHandshakeSuccess(conn) // 报告握手成功
	if err != nil {                      // 如果报告失败
		return err // 返回错误
	}

	return bufio.CopyPacketConn(ctx, uot.NewConn(conn, *request), bufio.NewPacketConn(c)) // 在UoT连接和UDP连接之间转发数据
}

// proxyOutboundTCPWithOutbound 使用指定的outbound处理TCP出站代理
func proxyOutboundTCPWithOutbound(ctx context.Context, conn net.Conn, destination M.Socksaddr, outbound C.Outbound) error {
	c, err := outbound.TCP(destination.AddrString())
	if err != nil {
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	err = N.ReportHandshakeSuccess(conn)
	if err != nil {
		return err
	}

	return bufio.CopyConn(ctx, conn, c)
}

// proxyOutboundUoTWithOutbound 使用指定的outbound处理UDP over TCP出站代理
func proxyOutboundUoTWithOutbound(ctx context.Context, conn net.Conn, destination M.Socksaddr, outbound C.Outbound) error {
	// 读取UoT请求
	request, err := uot.ReadRequest(conn)
	if err != nil {
		logrus.Debugln("proxyOutboundUoTWithOutbound ReadRequest:", err)
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	// 使用outbound创建UDP连接
	c, err := outbound.UDP(destination.AddrString())

	if err != nil {
		logrus.Debugln("proxyOutboundUoTWithOutbound UDP:", err)
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	err = N.ReportHandshakeSuccess(conn)
	if err != nil {
		return err
	}

	packetConn := &udpConnAdapter{UDPConn: c}

	// 在UoT连接和UDP连接之间转发数据
	return bufio.CopyPacketConn(ctx, uot.NewConn(conn, *request), bufio.NewPacketConn(packetConn))
}

// 直接使用 bufio.NewPacketConn 包装
