package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

func (c *Client) get_req(nc *NConn, key string) error {
	err_chan := make(chan error)
	go func () {
		buf := &bytes.Buffer{}
		if _, err := fmt.Fprintf(buf, "gets %s\r\n", key); err != nil {
			err_chan <- err
			return
		}

		// write the bytes to the connection
		if _, err := nc.buf.Write(buf.Bytes()); err != nil {
			err_chan <- err 
			return
		}

		if err := nc.buf.Flush(); err != nil {
			err_chan <- err
			return
		}

		err_chan <- nil
		return
	}()

	res_err := <- err_chan 
	if res_err != nil {
		return res_err
	}
	return nil
}

type itemChan struct {
	item	*Item
	err		error
}

func (c *Client) scan_item(line []byte) (*Item, uint32, error) {
	// extract the bytes
	item := &Item{}
	var len_res uint32
	pattern := "VALUE %s %d %d %d\r\n"
	dest := []any{&item.Key, &item.Flags, &len_res, &item.casid}

	if bytes.Count(line, []byte{byte(' ')}) == 3 {
		// the <casid> is not returned
		pattern = "VALUE %s %d %d\r\n"
		dest = dest[:3]
	}

	if _, err := fmt.Sscanf(string(line), pattern, dest...); err != nil {
		return nil, 0, err
	}
	return item, len_res, nil
}

func (c* Client) get_res(nc *NConn) (*Item, error) {
	item_chan := make(chan itemChan)
	go func () {
		// read the first line: VALUE <key> <flags> <bytes>(not including the delimiting \r\n) [<cas_unique>]\r\n
		first_line, err := nc.buf.ReadBytes('\n')
		if err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		// check if the response is END
		if bytes.Equal(first_line, END) {
			item_chan <- itemChan{err: ErrCacheMiss, item: nil}
			return
		}

		// extract the bytes
		item := &Item{}
		var len_res uint32
		pattern := "VALUE %s %d %d %d\r\n"
		dest := []any{&item.Key, &item.Flags, &len_res, &item.casid}

		fmt.Println(string(first_line))

		if bytes.Count(first_line, []byte{byte(' ')}) == 3 {
			// the <casid> is not returned
			pattern = "VALUE %s %d %d\r\n"
			dest = dest[:3]
		}

		if _, err := fmt.Sscanf(string(first_line), pattern, dest...); err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		value_buf := make([]byte, len_res + 2)
		if _, err := io.ReadFull(nc.buf.Reader, value_buf); err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		if !bytes.Equal(value_buf[len_res:], CRLF) {
			item_chan <- itemChan{err: errors.New("memcached: malformed response"), item: nil}
			return
		}

		item.Value = value_buf[:len_res]
		
		// now check if there is and END
		end_line, err := nc.buf.ReadBytes('\n')
		if err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		if !bytes.Equal(end_line, END) {
			item_chan <- itemChan{err: errors.New("memcached: malformed response"), item: nil}
			return
		}
		item_chan <- itemChan{err: nil, item: item}
	}()
	item_res := <- item_chan
	if item_res.err != nil {
		return nil, item_res.err
	}
	return item_res.item, item_res.err
}

func (c *Client) Get(key string) (*Item, error) {
	// first pick a server
	saddr, err := c.server_list.PickServer(key)
	if err != nil {
		return nil, err
	}

	// now get a connection
	conn, err := c.getConn(saddr)
	if err != nil {
		return nil, err
	}


	// send the get request
	if err := c.get_req(conn, key); err != nil {
		return nil, err	
	}

	item, err := c.get_res(conn)
	if err != nil {
		return nil, err
	}

	if err := c.putConn(conn); err != nil {
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
	}

	return item, nil
}

func (c *Client) gat_req(conn *NConn, key string, exptime int32) error {
	err_chan := make(chan error)
	go func () {
		buf := &bytes.Buffer{}
		if _, err := fmt.Fprintf(buf, "gat %d %s\r\n", exptime, key); err != nil {
			err_chan <- err
			return
		}

		// write the bytes to the connection
		if _, err := conn.buf.Write(buf.Bytes()); err != nil {
			err_chan <- err 
			return
		}

		if err := conn.buf.Flush(); err != nil {
			err_chan <- err
			return
		}

		err_chan <- nil
		return
	}()

	res_err := <- err_chan 
	if res_err != nil {
		return res_err
	}
	return nil
}

func (c *Client) gat_res(conn *NConn) (*Item, error) {
	item_chan := make(chan itemChan)
	go func () {
		// read the first line: VALUE <key> <flags> <bytes>(not including the delimiting \r\n) [<cas_unique>]\r\n
		first_line, err := conn.buf.ReadBytes('\n')
		if err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		// check if the response is END
		if bytes.Equal(first_line, END) {
			item_chan <- itemChan{err: ErrCacheMiss, item: nil}
			return
		}

		item, len_res, err := c.scan_item(first_line)
		if err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		// read the datablock
		value_buf := make([]byte, len_res + 2)
		if _, err := io.ReadFull(conn.buf, value_buf); err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		item.Value = value_buf[:len_res]

		// check for the end
		buf, err := conn.buf.ReadBytes('\n')
		if err != nil {
			item_chan <- itemChan{err: err, item: nil}
			return
		}

		if !bytes.Equal(buf, END) {
			item_chan <- itemChan{err: ErrServerError, item: nil}
			return
		}

		item_chan <- itemChan{err: nil, item: item}
		return

	}()

	res := <- item_chan
	if res.err != nil {
		return nil, res.err
	}

	return res.item, res.err
}

func (c *Client) Gat(key string, exptime int32) (*Item, error) {
	saddr, err := c.server_list.PickServer(key)
	if err != nil {
		return nil, err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return nil, err
	}

	if err := c.gat_req(conn, key, exptime); err != nil {
		return nil, err
	}

	item, err := c.gat_res(conn)
	if err != nil {
		return nil, err
	}

	if err := c.putConn(conn); err != nil {
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
	}

	return item, nil
}

func (c *Client) get_multi_req(conn *NConn, keys ...string) error {
	err_chan := make(chan error)
	go func() {
		if _, err := fmt.Fprintf(conn.buf, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
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

type multiRes struct {
	values 	map[string]*Item
	err 	error
}

func (c *Client) get_multi_res(conn *NConn) (map[string]*Item, error) {
	res_chan := make(chan multiRes)	
	go func ()  {
		values := make(map[string]*Item)
		for {
			buf, err := conn.buf.ReadBytes('\n')
			if err != nil {
				res_chan <- multiRes{err: err}
				return
			}
			if bytes.Equal(buf, END) {
				res_chan <- multiRes{values: values, err: nil}
				return
			}

			item := &Item{}
			var len_res uint32
			pattern := "VALUE %s %d %d %d\r\n"
			dest := []any{&item.Key, &item.Flags, &len_res, &item.casid}

			if bytes.Count(buf, []byte{byte(' ')}) == 3 {
				// the <casid> is not returned
				pattern = "VALUE %s %d %d\r\n"
				dest = dest[:3]
			}

			if _, err := fmt.Sscanf(string(buf), pattern, dest...); err != nil {
				res_chan <- multiRes{err: err}
				return
			}

			value_buf := make([]byte, len_res + 2)
			if _, err := io.ReadFull(conn.buf, value_buf); err != nil {
				res_chan <- multiRes{err: err}
				return
			}

			// add the item to the list
			values[item.Key] = item
		}
	}()
	res := <- res_chan
	return res.values, res.err
}

func (c *Client) GetMulti(keys ...string) (map[string]*Item, error) {
	saddr, err := c.server_list.PickServer(keys[0])
	if err != nil {
		return nil, err
	}

	conn, err := c.getConn(saddr)
	if err != nil {
		return nil, err
	}

	if err := c.get_multi_req(conn, keys...); err != nil {
		return nil, err
	}

	values, err := c.get_multi_res(conn)
	if err != nil {
		return nil, err
	}

	if err := c.putConn(conn); err != nil {
		if err := conn.c.Close(); err != nil {
			fmt.Println(err)
		}
	}

	return values, nil

}