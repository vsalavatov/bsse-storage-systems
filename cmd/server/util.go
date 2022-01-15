package main

import (
	"fmt"
	"net"
)

func debug(a ...interface{}) {
	fmt.Println("[DEBUG] ", a)
}

func logErr(conn net.Conn, args ...interface{}) {
	fmt.Println("failed to service ", conn.RemoteAddr().String(), ": ", args)
}
