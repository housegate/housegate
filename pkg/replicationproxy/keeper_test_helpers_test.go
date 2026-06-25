package replicationproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type keeperProxyFixture struct {
	addr string
	done <-chan error
	exit <-chan struct{}
}

type keeperUpstreamFixture struct {
	addr     string
	accepted chan struct{}
	observed chan []byte
}

func startKeeperProxy(t *testing.T, options KeeperOptions) keeperProxyFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return startKeeperProxyWithContext(t, ctx, options)
}

func startKeeperProxyWithContext(t *testing.T, ctx context.Context, options KeeperOptions) keeperProxyFixture {
	t.Helper()
	server, err := NewKeeperServer(options)
	if err != nil {
		t.Fatalf("NewKeeperServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	done := make(chan error, 1)
	exit := make(chan struct{})
	go func() {
		defer close(exit)
		done <- server.Serve(ctx, ln)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		select {
		case <-exit:
		case <-time.After(time.Second):
			t.Fatalf("proxy Serve did not exit")
		}
	})
	return keeperProxyFixture{addr: ln.Addr().String(), done: done, exit: exit}
}

func startKeeperEchoUpstream(t *testing.T) keeperUpstreamFixture {
	t.Helper()
	return startKeeperUpstream(t, func(conn net.Conn, observed chan<- []byte) {
		defer conn.Close()
		buf := make([]byte, 1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				payload := append([]byte(nil), buf[:n]...)
				observed <- payload
				if _, writeErr := conn.Write(payload); writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	})
}

func startKeeperEchoOnceUpstream(t *testing.T) keeperUpstreamFixture {
	t.Helper()
	return startKeeperUpstream(t, func(conn net.Conn, observed chan<- []byte) {
		defer conn.Close()
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if n > 0 {
			payload := append([]byte(nil), buf[:n]...)
			observed <- payload
			_, _ = conn.Write(payload)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}
	})
}

func startKeeperHoldingUpstream(t *testing.T) keeperUpstreamFixture {
	t.Helper()
	return startKeeperUpstream(t, func(conn net.Conn, observed chan<- []byte) {
		defer conn.Close()
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		if n > 0 {
			observed <- append([]byte(nil), buf[:n]...)
		}
		_, _ = io.Copy(io.Discard, conn)
	})
}

func startKeeperUpstream(t *testing.T, handle func(net.Conn, chan<- []byte)) keeperUpstreamFixture {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	accepted := make(chan struct{}, 4)
	observed := make(chan []byte, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			go handle(conn, observed)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
	})
	return keeperUpstreamFixture{addr: ln.Addr().String(), accepted: accepted, observed: observed}
}

func reserveClosedTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve closed addr: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}

func dialKeeperProxy(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	return conn
}

func writeAll(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
}

func readExactly(t *testing.T, conn net.Conn, size int) []byte {
	t.Helper()
	got := make([]byte, size)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return got
}

func readEOF(t *testing.T, conn net.Conn) error {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := conn.Read(make([]byte, 1))
	if errors.Is(err, io.EOF) || isClosedNetworkError(err) {
		return nil
	}
	return err
}

func receiveBytes(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case payload := <-ch:
		return payload
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upstream payload")
	}
	return nil
}

func requireAccepted(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upstream connection")
	}
}

func requireNoAcceptedConnection(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected mid-session reconnect to fallback upstream")
	default:
	}
}

func isClosedNetworkError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && !netErr.Timeout()
}
