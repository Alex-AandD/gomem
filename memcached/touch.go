package memcached

import (
	"bytes"
	"fmt"
)

func (c *Client) touch_req(conn *NConn, key string, exptime int32) error {
	err_chan := make(chan error)
	go func ()  {
		buf := &bytes.Buffer{}
		if _, err := fmt.Fprintf(buf, "touch %s %d\r\n", key, exptime); err != nil {
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

func (c *Client) touch_res(conn *NConn) error {
	err_chan := make(chan error)
	go func ()  {
		buf, err := conn.buf.ReadBytes('\n')
		if err != nil {
			err_chan <- err
			return
		}

		if bytes.Equal(buf, MEM_NOT_FOUND) {
			err_chan <- ErrNotFound
			return
		}

		if bytes.Equal(buf, MEM_TOUCHED) {
			err_chan <- nil
			return
		}
	}()

	res := <- err_chan
	return res
}


func (c *Client) Touch(key string, exptime int32) error {
	saddr, err := c.server_list.PickServer(key)
	if err != nil {
		return err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.touch_req(conn, key, exptime); err != nil {
		return err
	}

	if err := c.touch_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}