package memcached

import (
	"bytes"
	"fmt"
)

func (c *Client) cas_req(conn *NConn, item *Item) error {
	err_chan := make(chan error)
	go func ()  {
		if _, err := fmt.Fprintf(conn.buf, "cas %s %d %d %d %d\r\n", 
			item.Key, item.Flags, item.ExpTime, len(item.Value), item.casid); err != nil {
				err_chan <- err
				return
			}
		
		if _, err := conn.buf.Write(item.Value); err != nil {
			err_chan <- err
			return
		}

		if _, err := conn.buf.Write([]byte("\r\n")); err != nil {
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

func (c *Client) cas_res(conn *NConn) error {
	err_chan := make(chan error)
	go func () {
		buf, err := conn.buf.ReadBytes('\n')
		if err != nil {
			err_chan <- err
			return
		}

		if bytes.Equal(buf, MEM_STORED) {
			err_chan <- nil
			return
		}

		if bytes.Equal(buf, MEM_NOT_STORED) {
			err_chan <- ErrNotStored
			return
		}

		if bytes.Equal(buf, MEM_EXISTS) {
			err_chan <- ErrExists
			return
		}

		if bytes.Equal(buf, MEM_NOT_FOUND) {
			err_chan <- ErrNotFound
			return
		}

		err_chan <- ErrServerError
	}()

	res := <- err_chan
	return res
}

func (c *Client) Cas(item *Item) error {
	saddr, err := c.server_list.PickServer(item.Key)
	if err != nil {
		return err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.cas_req(conn, item); err != nil {
		return err
	}

	if err := c.cas_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
	}

	return nil
}