package replicationproxy

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

var ErrInvalidKeeperOptions = errors.New("replicationproxy: invalid keeper options")

type keeperUpstream struct {
	addr    string
	healthy bool
}

type keeperUpstreams struct {
	mu        sync.Mutex
	upstreams []keeperUpstream
	next      int
}

func newKeeperUpstreams(addrs []string) (*keeperUpstreams, error) {
	upstreams := make([]keeperUpstream, 0, len(addrs))
	for _, addr := range addrs {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("keeper upstream %q: %w", addr, ErrInvalidKeeperOptions)
		}
		upstreams = append(upstreams, keeperUpstream{addr: addr, healthy: true})
	}
	return &keeperUpstreams{upstreams: upstreams}, nil
}

func (s *keeperUpstreams) Candidates() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.upstreams) == 0 {
		return nil
	}

	healthy := s.candidatesLocked(true)
	if len(healthy) > 0 {
		return healthy
	}
	return s.candidatesLocked(false)
}

func (s *keeperUpstreams) candidatesLocked(onlyHealthy bool) []string {
	candidates := make([]string, 0, len(s.upstreams))
	for offset := range s.upstreams {
		idx := (s.next + offset) % len(s.upstreams)
		upstream := s.upstreams[idx]
		if onlyHealthy && !upstream.healthy {
			continue
		}
		candidates = append(candidates, upstream.addr)
	}
	if len(candidates) > 0 {
		s.next = (s.next + 1) % len(s.upstreams)
	}
	return candidates
}

func (s *keeperUpstreams) Mark(addr string, healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx := range s.upstreams {
		if s.upstreams[idx].addr == addr {
			s.upstreams[idx].healthy = healthy
			return
		}
	}
}
