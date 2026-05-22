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

	v4Addr := listeners[0].Addr().String()
	conn4, err := net.Dial("tcp4", v4Addr)
	if err != nil {
		t.Fatalf("failed to connect via IPv4: %v", err)
	}
	_ = conn4.Close()

	v6Addr := listeners[1].Addr().String()
	conn6, err := net.Dial("tcp6", v6Addr)
	if err != nil {
		t.Fatalf("failed to connect via IPv6: %v", err)
	}
	_ = conn6.Close()
}

func TestListenDualStack_PartialFailureStillWorks(t *testing.T) {
	ln, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	defer func() { _ = ln.Close() }()

	listeners, err := listenDualStack(port)
	if err != nil {
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

	for i, ln := range s.listeners {
		_, err := ln.Accept()
		if err == nil {
			t.Fatalf("listener %d still accepts after Close", i)
		}
	}
}

func TestServerClose_ReturnsQuickly(t *testing.T) {
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

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	select {
	case ac := <-accepted:
		defer func() { _ = ac.Close() }()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for accept")
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
		t.Fatal("Close 超过 1 秒未返回，说明仍在等待连接关闭")
	}
}
