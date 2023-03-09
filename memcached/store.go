package memcached

import (
	_"bufio"
	"bytes"
	"fmt"
	_"net"
)

func (c *Client) store_req(conn *NConn, cmd string, item *Item) error {
	// SET <key> <flags> <exptime> <bytes>\r\n
	err_chan := make(chan error)
	go func () {
		// write the byte
		buf := &bytes.Buffer{}
		if _, err := fmt.Fprintf(buf, "%s %s %d %d %d\r\n", cmd, item.Key, item.Flags, item.ExpTime, len(item.Value)); err != nil {
			err_chan <- err
			return
		}

		if _, err := conn.buf.Write(buf.Bytes()); err != nil {
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
	
	res_error := <- err_chan
	if res_error != nil {
		return res_error
	}	
	return nil

}

func (c *Client) store_res(conn *NConn) error {
	err_chan := make(chan error)
	go func () {
		var res []byte
		res, err := conn.buf.ReadBytes('\n')
		if err != nil {
			err_chan <- nil
			return
		}

		switch {
			case bytes.Equal(res, MEM_STORED): {
				err_chan <- nil
				return
			}

			case bytes.Equal(res, MEM_NOT_STORED): {
				err_chan <- ErrNotStored
				return
			}

			case bytes.Equal(res, MEM_ERROR): {
				err_chan <- ErrClientError
				return
			}
		}
		err_chan <- ErrServerError
		return
	}()

	res := <- err_chan
	if res != nil {
		return res
	}
	return nil
}


func (c *Client) Set(item *Item) error {
	saddr, err := c.server_list.PickServer(item.Key)
	if err != nil {
		return err
	}

	// get a connection
	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.store_req(conn, "set", item); err != nil {
		return err
	}

	if err := c.store_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
	}

	return nil
}

func (c *Client) Prepend(item *Item) error {
	saddr, err := c.server_list.PickServer(item.Key)
	if err != nil {
		return err
	}

	// get a connection
	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.store_req(conn, "PREPEND", item); err != nil {
		return err
	}

	if err := c.store_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
	}

	return nil
}
func (c *Client) Append(item *Item) error {
	saddr, err := c.server_list.PickServer(item.Key)
	if err != nil {
		return err
	}

	// get a connection
	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.store_req(conn, "APPEND", item); err != nil {
		return err
	}

	if err := c.store_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
	}
	return nil
}

func (c *Client) Add(item *Item) error {
	saddr, err := c.server_list.PickServer(item.Key)
	if err != nil {
		return err
	}

	// get a connection
	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.store_req(conn, "ADD", item); err != nil {
		return err
	}

	if err := c.store_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
	}
	return nil
}
func (c *Client) Replace(item *Item) error {
	saddr, err := c.server_list.PickServer(item.Key)
	if err != nil {
		return err
	}

	// get a connection
	conn, err := c.getConn(saddr)
	if err != nil {
		return err
	}

	if err := c.store_req(conn, "REPLACE", item); err != nil {
		return err
	}

	if err := c.store_res(conn); err != nil {
		return err
	}

	if err := c.putConn(conn); err != nil {
		fmt.Println(err)
	}
	return nil
}