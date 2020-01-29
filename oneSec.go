package main

import (
	"fmt"
	"time"
)

func main() {
	var i int64
	i = 0
	tick := time.Tick(1 * time.Second)
	for {
		i = i + 1
		select {
		case <-tick:
			goto  RETURN
		default:
		}
	}
	RETURN:
		fmt.Println(i)

}
