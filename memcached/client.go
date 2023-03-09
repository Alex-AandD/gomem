package memcached

import (
	"bufio"
	_ "bytes"
	"errors"
	_ "errors"
	"fmt"
	_ "log"
	"net"
	"sync"
)

type Client struct {
	max_idle_conns	int
	ntw 			string
	freemap 		map[string][]*NConn
	server_list 	*ServerList

	mu 				sync.RWMutex
}

func NewClient(servers ...string) (*Client, error) {
	c := &Client{
		max_idle_conns: DefaultMaxIdleConns,
		ntw: "tcp",
	}
	c.server_list= &ServerList{}
	err := c.server_list.SetServers(servers...)
	if err != nil {
		return nil, err
	}
	freemap := make(map[string][]*NConn)
	for _, server := range servers {
		freemap[server] = nil
	}
	c.freemap = freemap
	return c, nil
}


type NConn struct {
	c 		net.Conn
	saddr	net.Addr
	buf		*bufio.ReadWriter
}

type connChan struct {
	nc 		net.Conn
	err		error
}

type Item struct {
	Key 		string
	Value 		[]byte
	ExpTime		int32	
	Flags 		uint32
	casid 		uint64
}

func (i *Item) Casid() uint64 {
	return i.casid
}

func NewItem(key string, val []byte, exp_time int32, flags uint32, casid uint64) (*Item, error) {
	if key == "" {
		return nil, errors.New("client error: empty key")
	}

	if exp_time < 0 {
		fmt.Println("client warning: negative expiration time")
	}

	if len(val) == 0 {
		fmt.Println("client warning: empty value")
	}

	return &Item{
		ExpTime: exp_time,
		Flags: flags,
		casid: casid,
		Key: key,
		Value: val,
	}, nil
}
