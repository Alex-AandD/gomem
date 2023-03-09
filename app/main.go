package main

import (
	"fmt"
	"github.com/gomem/memcached"
	_"encoding/binary"
)

func main() {
	c, err := memcached.NewClient("localhost:11111")
	if err != nil {
		fmt.Printf(err.Error())
	}
	counter_1, err := memcached.NewItem("counter1", []byte("1"), 50 *50 * 5, 0, 0)
	if err != nil {
		fmt.Println(err)
		return
	}
	counter_2, err := memcached.NewItem("counter2", []byte("1"), 50 *50 * 5, 0, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := c.Set(counter_1); err != nil {
		fmt.Println(err)
		return
	}

	if err := c.Set(counter_2); err != nil {
		fmt.Println(err)
		return
	}

	values, err := c.GetMulti("counter1", "counter2")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(values)
	fmt.Println("done!")
}