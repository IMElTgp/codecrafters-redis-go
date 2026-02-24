package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage

	port, config := parseCLIArgs(os.Args)

	if config.port != strconv.Itoa(port) {
		// this server's master is not itself, which indicates that it is the slave
		serverRole = false
	}

	if config.port == "" {
		// fix: master itself may not provide replica, which leads config.port to be empty
		serverRole = true
	}

	// replica
	if !serverRole {
		conn, err := net.Dial("tcp", config.host+":"+config.port)
		if err != nil {
			// handle error
			return
		}

		// handshake from replica
		// master handles them in handleConn(l.Accept()) later
		_ = sendPING(config.host, config.port, conn)
		// read from master to avoid continuous writing
		_ = readFromMaster(conn)
		_ = sendREPLCONF1(strconv.Itoa(port), conn)
		_ = readFromMaster(conn)
		_ = sendREPLCONF2(conn)
		_ = readFromMaster(conn)
		_ = sendPSYNC(conn)
		_ = readFromMaster(conn)
	}

	address := "0.0.0.0:" + strconv.Itoa(port)

	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port" + strconv.Itoa(port))
		os.Exit(1)
	}

	defer func() {
		err = l.Close()
		if err != nil {
			// handle error
			os.Exit(1)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			// handle error
			return
		}

		// use goroutines to process multiple clients
		// redis uses event loop
		go handleConn(conn)
	}
}
