package server

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestListenDualStack_ListensOnBothIPv4AndIPv6(t *testing.T) {
	listeners, err := listenDualStack(0)
	if err != nil {
		t.Fatalf("listenDualStack failed: %v", err)
	}
	defer func() {
		for _, ln := range listeners {
			_ = ln.Close()
		}
	}()

	if len(listeners) < 2 {
		t.Skipf("dual-stack not available, got %d listener(s)", len(listeners))
	}

	// Verify we can connect via IPv4
	v4Addr := listeners[0].Addr().String()
	conn4, err := net.Dial("tcp4", v4Addr)
	if err != nil {
		t.Fatalf("failed to connect via IPv4: %v", err)
	}
	_ = conn4.Close()

	// Verify we can connect via IPv6
	v6Addr := listeners[1].Addr().String()
	conn6, err := net.Dial("tcp6", v6Addr)
	if err != nil {
		t.Fatalf("failed to connect via IPv6: %v", err)
	}
	_ = conn6.Close()
}

func TestListenDualStack_PartialFailureStillWorks(t *testing.T) {
	// Listen on a random port first to get a port number
	ln, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	// Keep v4 occupied so listenDualStack can only get v6
	defer func() { _ = ln.Close() }()

	listeners, err := listenDualStack(port)
	if err != nil {
		// On systems where IPv6 is also unavailable, this is acceptable
		t.Skipf("listenDualStack failed entirely (expected on some CI): %v", err)
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()

	if len(listeners) == 0 {
		t.Fatal("expected at least 1 listener when partial failure")
	}

	// Should have gotten at least the v6 listener
	for _, l := range listeners {
		addr := l.Addr().String()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to connect to %s: %v", addr, err)
		}
		_ = conn.Close()
	}
}

func TestListenDualStack_AllFailReturnsError(t *testing.T) {
	// Use an invalid port to force failure
	_, err := listenDualStack(-1)
	if err == nil {
		t.Fatal("expected error when all listeners fail, got nil")
	}
}

func TestServerClose_WithMultipleListeners(t *testing.T) {
	ln4, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen v4: %v", err)
	}
	ln6, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		_ = ln4.Close()
		t.Skipf("IPv6 not available: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		listeners: []net.Listener{ln4, ln6},
		ctx:       ctx,
		cancel:    cancel,
	}

	// Accept on both listeners in background
	for _, ln := range s.listeners {
		go func(l net.Listener) {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				_ = c.Close()
			}
		}(ln)
	}

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
		t.Fatal("Close did not return within 1 second")
	}

	// Verify both listeners are closed
	for i, ln := range s.listeners {
		_, err := ln.Accept()
		if err == nil {
			t.Fatalf("listener %d still accepts after Close", i)
		}
	}
}

func TestServerClose_ReturnsQuickly(t *testing.T) {
	// 启动一个真实的 TCP listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		listeners: []net.Listener{listener},
		ctx:       ctx,
		cancel:    cancel,
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
