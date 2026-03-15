package server

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestServerClose_ReturnsQuickly(t *testing.T) {
	// 启动一个真实的 TCP listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
	}

	// 模拟活跃连接：建立一个不会主动关闭的连接
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// 在后台 accept 这个连接，让 listener 正常工作
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	// 等待连接被 accept
	select {
	case ac := <-accepted:
		defer func() { _ = ac.Close() }()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for accept")
	}

	// 验证 Close 在 1 秒内返回（之前要等 30 秒）
	done := make(chan error, 1)
	go func() {
		done <- s.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close 超过 1 秒未返回，说明仍在等待连接关闭")
	}
}
