package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type Conn struct {
	Conn net.Conn
}

func (c *Conn) runPING() error {
	_, err := c.Conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runECHO(strs []string) error {
	for _, str := range strs {
		_, err := c.Conn.Write([]byte(str))
		if err != nil {
			// handle error
			return err
		}
	}

	return nil
}

// a RESP argument parser
func parseArgs(msg string) (args []string) {
	msg = strings.TrimSpace(msg)

	msgLen := len(msg)
	var arg string
	for i := range msgLen {
		if msg[i] == ' ' {
			args = append(args, arg)
			arg = ""
			continue
		}

		arg += string(msg[i])
	}

	if len(arg) > 0 {
		args = append(args, arg)
	}
	return

}

// working function which carries logics related to client serving
func handleConn(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			// handle error
			return
		}
	}()

	var buffer = make([]byte, 1024)
	c := &Conn{conn}

	for {
		// if _, err := conn.Read(buffer); err == io.EOF {
		// 	return
		// }

		_, err := conn.Read(buffer)
		if err != nil {
			// handle error
			if err == io.EOF {
				return
			}

			panic("err:" + err.Error())
		}

		// parse args
		// fmt.Println(string(buffer))
		panic("this is buffer:" + string(buffer))
		args := parseArgs(string(buffer))
		switch strings.ToUpper(args[1]) {
		case "PING":
			c.runPING()
		case "ECHO":
			c.runECHO(args[2:])
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
		// if _, err = conn.Read(buffer); err != nil {
		// 	return
		// }
		// try io.EOF
		conn, err := l.Accept()
		if err != nil {
			// handle error
			return
		}

		// use goroutines to process multiple clients
		go handleConn(conn)
	}
}
