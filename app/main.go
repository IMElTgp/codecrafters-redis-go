package main

import (
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Conn struct {
	Conn net.Conn
}

// Value is for expire time registering
type Value struct {
	Val string
	Ex  time.Time
}

// a global hash map for GET & SET
var variables sync.Map

// a global hash map for lists
var lists sync.Map

// tool function for string serialization
func serialize(str string) string {
	return "$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"
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
		_, err := c.Conn.Write([]byte(serialize(str)))
		if err != nil {
			// handle error
			return err
		}
	}

	return nil
}

func (c *Conn) runSET(args []string) error {
	if len(args) != 2 && len(args) != 4 {
		return fmt.Errorf("bad RESP array: argument count mismatch")
	}

	// variables.Store(args[0], args[1])
	expTime := 0
	var err error
	if len(args) == 4 {
		expTime, err = strconv.Atoi(args[3])
		if err != nil {
			// handle error
			return err
		}
	}

	val := Value{args[1], time.Now()}
	// set expire time
	if len(args) > 2 {
		switch strings.ToUpper(args[2]) {
		// AVOID val.Ex.Add(...)
		case "EX":
			val.Ex = time.Now().Add(time.Duration(expTime) * time.Second)
		case "PX":
			val.Ex = time.Now().Add(time.Duration(expTime) * time.Millisecond)
		default:
			val.Ex = time.Now().Add(time.Duration(math.MaxInt32) * time.Second)
		}
	} else {
		val.Ex = time.Now().Add(time.Duration(math.MaxInt32) * time.Second)
	}
	variables.Store(args[0], val)

	_, err = c.Conn.Write([]byte("+OK\r\n"))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runGET(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("bad RESP array: argument count mismatch")
	}

	v, ok := variables.Load(args[0])
	if !ok {
		_, err := c.Conn.Write([]byte("$-1\r\n"))
		if err != nil {
			// handle error
			return err
		}
		return nil
	}

	val, ok := v.(Value)
	if !ok {
		return fmt.Errorf("got bad type")
	}
	if time.Now().After(val.Ex) {
		// expires
		variables.Delete(args[0])
		_, err := c.Conn.Write([]byte("$-1\r\n"))
		if err != nil {
			// handle error
			return err
		}
		return nil
	}
	_, err := c.Conn.Write([]byte(serialize(val.Val)))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runRPUSH(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("RPUSH: bad argument count")
	}

	list, ok := lists.Load(args[0])
	if !ok {
		lists.Store(args[0], []any{})
		list, _ = lists.Load(args[0])
	}
	l, ok := list.([]any)
	if !ok {
		return fmt.Errorf("RPUSH: wrong list type")
	}
	for _, arg := range args[1:] {
		l = append(l, arg)
	}
	lists.Store(args[0], l)
	listLen := len(l)
	_, err := c.Conn.Write([]byte(":" + strconv.Itoa(listLen) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runLPUSH(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("RPUSH: bad argument count")
	}

	list, ok := lists.Load(args[0])
	if !ok {
		lists.Store(args[0], []any{})
		list, _ = lists.Load(args[0])
	}
	l, ok := list.([]any)
	if !ok {
		return fmt.Errorf("RPUSH: wrong list type")
	}
	for _, arg := range args[1:] {
		l = append([]any{arg}, l...)
	}
	lists.Store(args[0], l)
	listLen := len(l)
	_, err := c.Conn.Write([]byte(":" + strconv.Itoa(listLen) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runLRANGE(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("LRANGE: bad argument count")
	}

	list, ok := lists.Load(args[0])
	if !ok {
		// return fmt.Errorf("LRANGE: no such list")
		// here we should create a list
		// like os.O_CREATE
		lists.Store(args[0], []any{})
		list, _ = lists.Load(args[0])
	}

	l, ok := list.([]any)
	if !ok {
		return fmt.Errorf("LRANGE: list type mismatch")
	}

	// usage: LRANGE l 0 2 -> get a RESP array that lists 3 elements of
	// l beginning from index 0 and ending at index 2
	lBoarder, err := strconv.Atoi(args[1])
	if err != nil {
		// handle error
		return err
	}
	rBoarder, err := strconv.Atoi(args[2])
	if err != nil {
		// handle error
		return err
	}

	// negative indices
	if lBoarder < 0 {
		lBoarder += len(l)
		// out of range: treated as 0
		if lBoarder < 0 {
			lBoarder = 0
		}
	}
	if rBoarder < 0 {
		rBoarder += len(l)
		if rBoarder < 0 {
			rBoarder = 0
		}
	}

	// corner cases
	if lBoarder >= len(l) {
		_, err = c.Conn.Write([]byte("*0\r\n"))
		return fmt.Errorf("LRANGE: out of range")
	}
	rBoarder = int(math.Min(float64(rBoarder), float64(len(l)-1)))

	_, err = c.Conn.Write([]byte("*" + strconv.Itoa(rBoarder-lBoarder+1) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}
	for _, elem := range l[lBoarder : rBoarder+1] {
		_, err = c.Conn.Write([]byte(serialize(elem.(string))))
		if err != nil {
			// handle error
			return err
		}
	}

	return nil
}

func (c *Conn) runLLEN(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("LLEN: argument count mismatch")
	}

	list, ok := lists.Load(args[0])
	if !ok {
		_, err := c.Conn.Write([]byte(":0\r\n"))
		if err != nil {
			// handle error
			return err
		}
	}

	l, ok := list.([]any)
	if !ok {
		return fmt.Errorf("LLEN: list type mismatch")
	}
	_, err := c.Conn.Write([]byte(":" + strconv.Itoa(len(l)) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runLPOP(args []string) error {

	list, ok := lists.Load(args[0])
	if !ok {
		_, err := c.Conn.Write([]byte("$-1\r\n"))
		if err != nil {
			// handle error
			return err
		}
	}

	l, ok := list.([]any)
	if !ok {
		return fmt.Errorf("LPOP: list type mismatch")
	}

	var (
		toPop = 1
		err   error
	)
	if len(args) > 1 {
		toPop, err = strconv.Atoi(args[1])
		if err != nil {
			// handle error
			return err
		}
	}

	// pop from left
	if len(args) > 1 {
		_, err = c.Conn.Write([]byte("*" + strconv.Itoa(toPop) + "\r\n"))
		if err != nil {
			// handle error
			return err
		}
	}
	for i := 0; i < toPop; i++ {
		_, err = c.Conn.Write([]byte(serialize(l[0].(string))))
		if err != nil {
			// handle error
			return err
		}
		l = l[1:]
	}
	lists.Store(args[0], l)

	return nil
}

// a RESP argument parser
func parseArgs(msg string) (args []string, consumed int, err error) {
	// msg = strings.TrimSpace(msg)
	// general rule of REdis Serialization Protocol (RESP) array
	// *<count>\r\n followed by that many elements
	// for each element: $<len>\r\n<bytes>\r\n
	// for example: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
	if msg[0] != '*' {
		return nil, 0, fmt.Errorf("bad RESP array: syntax error")
	}

	argCntBegin, argCntEnd := -1, -1
	for i, b := range msg {
		if b == '*' && argCntBegin == -1 {
			argCntBegin = i + 1
		}
		if b == '\r' && argCntEnd == -1 {
			argCntEnd = i
			break
		}
	}
	if argCntEnd == -1 || argCntBegin == -1 || argCntBegin >= argCntEnd {
		return nil, 0, fmt.Errorf("bad RESP array: syntax error")
	}

	argCnt, err := strconv.Atoi(msg[argCntBegin:argCntEnd])
	if err != nil {
		// handle error
		return nil, 0, fmt.Errorf("bad RESP array: syntax error")
	}

	if argCnt == 0 {
		// *0\r\n
		return []string{}, 4, nil
	}

	i := 0
	for i = 1; len(args) < argCnt && i < len(msg); i++ {
		b := msg[i]
		if b == '*' {
			// crossing of multiple commands
			return nil, 0, fmt.Errorf("bad RESP array: syntax error")
		}
		if b == '$' {
			j := i
			for j < len(msg) && msg[j] != '\r' {
				j++
			}
			argLen, err := strconv.Atoi(msg[i+1 : j])
			// notice those \r's and \n's
			if err != nil || len(msg) < j+4+argLen {
				// handle error
				return nil, 0, fmt.Errorf("bad RESP array: syntax error")
			}
			// ensure framing
			if msg[j:j+2] != "\r\n" || msg[j+2+argLen:j+4+argLen] != "\r\n" {
				return nil, 0, fmt.Errorf("bad RESP array: syntax error")
			}

			args = append(args, msg[j+2:j+2+argLen])
			// skip to the next $-prefix to avoid direct visit to payload
			// which avoids mishandling of nested '$'
			i = j + 3 + argLen
		}
	}

	if len(args) != argCnt {
		return nil, 0, fmt.Errorf("bad RESP array: argument count mismatch")
	}
	consumed = i
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
		n, err := conn.Read(buffer)
		if err != nil {
			// handle error
			if err == io.EOF {
				return
			}

			panic("err:" + err.Error())
		}

		// parse args
		totalConsumed := 0
		for totalConsumed < len(buffer[:n]) {
			args, consumed, err := parseArgs(string(buffer[totalConsumed:n]))
			if err != nil {
				// handle error
				return
			}
			if len(args) == 0 {
				return
			}
			switch strings.ToUpper(args[0]) {
			case "PING":
				if c.runPING() != nil {
					// handle error
					return
				}
			case "ECHO":
				if c.runECHO(args[1:]) != nil {
					// handle error
					return
				}
			case "SET":
				if c.runSET(args[1:]) != nil {
					// handle error
					return
				}
			case "GET":
				if c.runGET(args[1:]) != nil {
					// handle error
					return
				}
			case "RPUSH":
				if c.runRPUSH(args[1:]) != nil {
					return
				}
			case "LPUSH":
				if c.runLPUSH(args[1:]) != nil {
					return
				}
			case "LRANGE":
				if c.runLRANGE(args[1:]) != nil {
					return
				}
			case "LLEN":
				if c.runLLEN(args[1:]) != nil {
					return
				}
			case "LPOP":
				if c.runLPOP(args[1:]) != nil {
					return
				}
			}

			totalConsumed += consumed
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
		conn, err := l.Accept()
		if err != nil {
			// handle error
			return
		}

		// use goroutines to process multiple clients
		// real redis uses event loop
		go handleConn(conn)
	}
}
