package server

import (
	"context"
	"net"

	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
)

func proxyOutboundTCPWithOutbound(ctx context.Context, conn net.Conn, destination M.Socksaddr, outbound Outbound) error {
	c, err := outbound.TCP(destination.String())
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

func proxyOutboundUoTWithOutbound(ctx context.Context, conn net.Conn, destination M.Socksaddr, outbound Outbound) error {
	request, err := uot.ReadRequest(conn)
	if err != nil {
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	c, err := outbound.UDP(destination.String())
	if err != nil {
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	err = N.ReportHandshakeSuccess(conn)
	if err != nil {
		return err
	}

	packetConn := &udpConnAdapter{UDPConn: c}
	return bufio.CopyPacketConn(ctx, uot.NewConn(conn, *request), bufio.NewPacketConn(packetConn))
}
