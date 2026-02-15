package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
		// _, err := c.Conn.Write([]byte(str))
		var sendback []byte
		sendback = append(sendback, '$')
		sendback = append(sendback, '\r')
		sendback = append(sendback, '\n')
		sendback = append(sendback, []byte(string(len(str)))...)
		sendback = append(sendback, '\r')
		sendback = append(sendback, '\n')
		_, err := c.Conn.Write(sendback)
		if err != nil {
			// handle error
			return err
		}
	}

	return nil
}

// a RESP argument parser
func parseArgs(msg string) (args []string, err error) {
	// msg = strings.TrimSpace(msg)
	// general rule of REdis Serialization Protocol (RESP) array
	// *<count>\r\n followed by that many elements
	// for each element: $<len>\r\n<bytes>\r\n
	// for example: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
	argCnt, err := strconv.Atoi(string(msg[1]))
	if err != nil {
		// handle error
		return nil, fmt.Errorf("bad RESP array: syntax error")
	}

	for i, b := range msg {
		if b == '$' {
			argLen, err := strconv.Atoi(string(msg[i+1]))
			// notice those \r's and \n's
			if err != nil || len(msg) < i+6+argLen {
				// handle error
				return nil, fmt.Errorf("bad RESP array: syntax error")
			}

			args = append(args, msg[i+4:i+4+argLen])
		}
	}

	if len(args) != argCnt {
		return nil, fmt.Errorf("bad RESP array: argument count mismatch")
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

		n, err := conn.Read(buffer)
		if err != nil {
			// handle error
			if err == io.EOF {
				return
			}

			panic("err:" + err.Error())
		}

		// parse args
		// fmt.Println(string(buffer))
		// panic("this is buffer:" + string(buffer))
		args, err := parseArgs(string(buffer[:n]))
		if err != nil {
			// handle error
			return
		}
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
