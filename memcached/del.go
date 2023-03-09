package memcached

import (
	"bytes"
	"fmt"
)

func (c *Client) del_req(conn *NConn, key string) error {
	err_chan := make(chan error)
	go func () {
		buf := &bytes.Buffer{}
		if _, err := fmt.Fprintf(buf, "delete %s\r\n", key); err != nil {
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
	res := <- err_chan
	return res
}

func (c *Client) del_res(conn *NConn) error {
	err_chan := make(chan error)
	go func ()  {
		buf, err := conn.buf.ReadBytes('\n')
		if err != nil {
			err_chan <- err
			return
		}

		if bytes.Equal(buf, MEM_DELETED) {
			err_chan <- nil
			return
		}

		if bytes.Equal(buf, MEM_NOT_FOUND) {
			err_chan <- ErrNotFound
			return
		}

	}()
	res := <- err_chan
	return res
}

func (c *Client) Delete(key string) error {
	saddr, err := c.server_list.PickServer(key)
	if err != nil {
		return err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.del_req(conn, key); err != nil {
		return err
	}

	if err := c.del_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
		fmt.Println(err)
	}
	return nil
}
