package memcached
import (
	"net"
	"bufio"
	_"fmt"
)

func OpenConnection(s_addr net.Addr) (*NConn, error) {
	var c_chan = make(chan connChan)
	go func () {
		conn, err := net.Dial(s_addr.Network(), s_addr.String())
		if err != nil {
			c_chan <- connChan {
				nc: nil,
				err: err,
			}
			return
		}

		c_chan <- connChan{
			nc: conn,
			err: nil,
		}
	}()

	res := <- c_chan
	if res.err != nil {
		return nil, res.err
	}

	nc := &NConn{
		c: res.nc,
		saddr: s_addr,
		buf: bufio.NewReadWriter(bufio.NewReader(res.nc), bufio.NewWriter(res.nc)),
	}
	return nc, nil
}
func (c *Client) putConn(conn *NConn) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	saddr := conn.saddr
	freelist := c.freemap[saddr.String()]

	if freelist == nil {
		freelist = []*NConn{}
	}

	freelist = append(freelist, conn)
	c.freemap[saddr.String()] = freelist 

	return nil
}

func (c *Client) getConn(saddr net.Addr) (*NConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.freemap == nil {
		// create a new map
		c.freemap = make(map[string][]*NConn)
	}

	freelist := c.freemap[saddr.String()]

	if freelist	== nil || len(freelist) == 0 {
		// create freelist
		conn, err := OpenConnection(saddr)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}

	conn := freelist[len(freelist) - 1]
	freelist = freelist[:len(freelist) - 1]

	c.freemap[saddr.String()] = freelist

	return conn, nil
}