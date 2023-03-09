package memcached

import (
	"bytes"
	"fmt"
)

func (c *Client) counter_req(conn *NConn, cmd string, key string, val uint64) error {
	err_chan := make(chan error)
	go func ()  {
		buf := &bytes.Buffer{}
		if _, err := fmt.Fprintf(buf, "%s %s %d\r\n", cmd, key, val); err != nil {
			err_chan <- err
			return
		}

		if _, err := conn.buf.Write(buf.Bytes()); err != nil {
			err_chan <- err
			return
		}

		if err := conn.buf.Flush(); err != nil {
			err_chan <- err
			return
		}

		err_chan <- nil
	}()
	err := <- err_chan
	return err
}

type cntChan struct {
	value 	uint64
	err 	error 
}

func (c *Client) counter_res(conn *NConn, val uint64) (uint64, error) {
	cnt_chan := make(chan cntChan)

	go func ()  {
		buf, err := conn.buf.ReadBytes('\n')
		if err != nil {
			cnt_chan <- cntChan{err: err}
			return
		}

		if bytes.Equal(buf, MEM_NOT_FOUND) {
			cnt_chan <- cntChan{err: ErrNotFound}
			return
		}
		var value uint64
		if _, err := fmt.Sscanf(string(buf), "%d\r\n", &value); err != nil {
			cnt_chan <- cntChan{err: err}
			return
		}
		cnt_chan <- cntChan{err: nil, value: value}
	}()

	res := <- cnt_chan
	if res.err != nil {
		return 0, res.err
	}
	return res.value, nil
}

func (c *Client) Incr(key string, val uint64) (uint64, error) {
	saddr, err := c.server_list.PickServer(key)
	if err != nil {
		return 0, err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return 0, err
	}

	if err := c.counter_req(conn, "incr", key, val); err != nil {
		return 0, err
	}

	value, err := c.counter_res(conn, val)
	if err != nil {
		return 0, err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
	}
	return value, err
}

func (c *Client) Decr(key string, val uint64) (uint64, error) {
	saddr, err := c.server_list.PickServer(key)
	if err != nil {
		return 0, err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return 0, err
	}

	if err := c.counter_req(conn, "decr", key, val); err != nil {
		return 0, err
	}

	value, err := c.counter_res(conn, val)
	if err != nil {
		return 0, err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
	}

	return value, err
}