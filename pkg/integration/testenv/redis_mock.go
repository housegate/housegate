package testenv

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/config"
)

// WithMiniredis starts an in-process Redis fake (miniredis) and returns
// a ProxyOption that wires it as the proxy's default redis backend.
// Both cfg.RedisDefaultAddr and opts.RedisClients are set so the
// internal redisFactory finds the pre-dialed client and skips the dial.
//
// Use for plugins that need Redis (concurrency, anything that calls
// redisFactory.get) in tests where standing up a real Redis would be
// overkill.
func WithMiniredis(t *testing.T) ProxyOption {
	t.Helper()
	opt, _ := WithMiniredisHandle(t)
	return opt
}

// WithMiniredisHandle is the same as WithMiniredis but also returns the
// underlying *miniredis.Miniredis so a test can close it mid-run to
// simulate a Redis outage. The auto-Close on t.Cleanup is still
// registered; a second Close on an already-closed instance is a no-op.
func WithMiniredisHandle(t *testing.T) (ProxyOption, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("miniredis ping: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	opt := func(cfg *config.Config, opts *housegate.Options) {
		cfg.RedisDefaultAddr = mr.Addr()
		if opts.RedisClients == nil {
			opts.RedisClients = map[string]*redis.Client{}
		}
		// Key must match the address the redis factory resolves to.
		// ResolveRedisAddr("") falls back to cfg.RedisDefaultAddr,
		// which we just set to mr.Addr() above.
		opts.RedisClients[mr.Addr()] = client
	}
	return opt, mr
}
