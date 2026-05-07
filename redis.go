package housegate

import (
	"context"
	"errors"
	"fmt"

	"sentioxyz/sentio-core/common/log"

	"github.com/redis/go-redis/v9"

	"housegate/housegate/pkg/config"
)

// redisFactory dedupes redis client construction by resolved address.
// Several config sections (network_state, usage, concurrency_limit)
// may point at the same redis host; we open one pool per resolved
// addr.
//
// libBuilt is the set of resolved addresses for which the factory
// dialed the client itself (vs. accepting one via Options.RedisClients).
// Only lib-built clients are Close()d in closeAll.
type redisFactory struct {
	cfg      *config.Config
	clients  map[string]*redis.Client
	libBuilt map[string]bool
}

func newRedisFactory(cfg *config.Config, preDialed map[string]*redis.Client) *redisFactory {
	f := &redisFactory{
		cfg:      cfg,
		clients:  map[string]*redis.Client{},
		libBuilt: map[string]bool{},
	}
	for addr, c := range preDialed {
		f.clients[addr] = c
		// libBuilt stays false for caller-supplied entries.
	}
	return f
}

// get returns a redis client for the resolved addr. sectionAddr falls
// back to RedisDefaultAddr when blank. An empty resolved addr is an
// error.
func (f *redisFactory) get(sectionAddr string) (*redis.Client, error) {
	addr := f.cfg.ResolveRedisAddr(sectionAddr)
	if addr == "" {
		return nil, errors.New("no redis address configured (set section's redis_addr or top-level redis_default_addr)")
	}
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis %s: %w", addr, err)
	}
	f.clients[addr] = c
	f.libBuilt[addr] = true
	return c, nil
}

// closeAll closes only lib-built clients; caller-supplied entries are
// preserved so the caller can keep using them after Run returns.
func (f *redisFactory) closeAll() {
	for addr, c := range f.clients {
		if !f.libBuilt[addr] {
			continue
		}
		if err := c.Close(); err != nil {
			log.Warnfe(err, "redis close addr=%v", addr)
		}
	}
}
