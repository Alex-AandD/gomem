package memcached

import (
	"net"
	"sync"
	"errors"
	"fmt"
	"hash/crc32"
)
type Selector interface {
	PickServer(key string) (net.Addr, error)
}

type ServerList struct {
	addrs 		[]net.Addr
	mu			sync.RWMutex
}

// TODO: ADD capability of scaling the servers at runtime (add, remove)
func (s *ServerList) SetServers(servers ...string) error {
	// create a list with length of servers
	ss := make([]net.Addr, len(servers))
	for i := 0; i < len(servers); i++ {
		addr, err := net.ResolveTCPAddr("tcp", servers[i])
		if err != nil {
			return errors.New(fmt.Sprintf("config: %s", err.Error()))
		}
		// add the address
		ss[i] = addr 
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = ss
	return nil
}

func (s *ServerList) PickServer(key string) (net.Addr, error) {
	// check if the key is valid
	if len(key) > 250 {
		return nil, ErrMalformedKey
	}
	for _, char := range(key) {
		if char <= 32 {
			return nil, ErrMalformedKey
		}
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	index := hash % uint32(len(s.addrs))
	return s.addrs[index], nil
}