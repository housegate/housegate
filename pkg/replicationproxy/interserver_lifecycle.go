package replicationproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/log"
)

func (s *InterserverServer) Serve(ctx context.Context, ln net.Listener) error {
	ctx, logger := log.FromContext(ctx, "component", "replicationproxy_interserver", "listen", ln.Addr().String())
	httpServer := &http.Server{
		Handler:      s.Handler(),
		ReadTimeout:  s.readTimeout,
		WriteTimeout: s.writeTimeout,
	}
	done := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			shutdownDone <- httpServer.Shutdown(shutdownCtx)
		case <-done:
			shutdownDone <- nil
		}
	}()

	err := httpServer.Serve(ln)
	close(done)
	shutdownErr := <-shutdownDone
	if shutdownErr != nil {
		logger.Warnw("interserver shutdown failed", "error", shutdownErr)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve interserver http: %w", err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func isLoopbackRemoteAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
