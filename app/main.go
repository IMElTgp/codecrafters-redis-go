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

// KV is for entry content
type KV struct {
	key   string
	value string
}

// Entry is for entry type
type Entry struct {
	id string
	kv []KV
}

// Stream is type stream
type Stream []Entry

// a global hash map for GET & SET
var variables sync.Map

// a global hash map for lists
var lists sync.Map

// a global has map for streams
var streams sync.Map

// a global mutex to ensure concurrency-safety
var mu sync.Mutex

// a global hash map for notifying BLPOP
var notify sync.Map // Map[string]chan struct{}

// tool function for getting list copy from Map
func getCopy(key string) ([]any, error) {
	list, ok := lists.Load(key)
	if !ok {
		lists.Store(key, []any{})
		list, _ = lists.Load(key)
	}
	l, ok := list.([]any)
	if !ok {
		return nil, fmt.Errorf("list type mismatch")
	}
	cp := append([]any(nil), l...)
	return cp, nil
}

// tool function for getting channel
func getCh(key string) chan struct{} {
	if v, ok := notify.Load(key); ok {
		return v.(chan struct{})
	}

	ch := make(chan struct{}, 1)
	v, _ := notify.LoadOrStore(key, ch)
	return v.(chan struct{})
}

// tool function for string serialization (return a bulk string)
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
	mu.Lock()
	variables.Store(args[0], val)
	mu.Unlock()

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

	mu.Lock()
	v, ok := variables.Load(args[0])
	mu.Unlock()
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
		mu.Lock()
		variables.Delete(args[0])
		mu.Unlock()
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

	mu.Lock()
	cp, err := getCopy(args[0])
	if err != nil {
		// handle error
		mu.Unlock()
		return err
	}

	appended := false

	for _, arg := range args[1:] {
		cp = append(cp, arg)
		appended = true
	}

	lists.Store(args[0], cp)
	listLen := len(cp)
	ch := getCh(args[0])
	mu.Unlock()

	if appended {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	_, err = c.Conn.Write([]byte(":" + strconv.Itoa(listLen) + "\r\n"))
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

	mu.Lock()
	cp, err := getCopy(args[0])
	if err != nil {
		// handle error
		mu.Unlock()
		return err
	}
	ch := getCh(args[0])
	appended := false
	for _, arg := range args[1:] {
		cp = append([]any{arg}, cp...)
		appended = true
	}

	lists.Store(args[0], cp)
	listLen := len(cp)
	mu.Unlock()

	if appended {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	_, err = c.Conn.Write([]byte(":" + strconv.Itoa(listLen) + "\r\n"))
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

	mu.Lock()
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
		mu.Unlock()
		return fmt.Errorf("LRANGE: list type mismatch")
	}
	cp := append([]any(nil), l...)

	// usage: LRANGE l 0 2 -> get a RESP array that lists 3 elements of
	// l beginning from index 0 and ending at index 2
	lBoarder, err := strconv.Atoi(args[1])
	if err != nil {
		mu.Unlock()
		// handle error
		return err
	}
	rBoarder, err := strconv.Atoi(args[2])
	if err != nil {
		mu.Unlock()
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
	ln := len(l)
	mu.Unlock()

	// corner cases
	if lBoarder >= ln {
		_, err = c.Conn.Write([]byte("*0\r\n"))
		return fmt.Errorf("LRANGE: out of range")
	}
	rBoarder = int(math.Min(float64(rBoarder), float64(ln-1)))

	_, err = c.Conn.Write([]byte("*" + strconv.Itoa(rBoarder-lBoarder+1) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}
	for _, elem := range cp[lBoarder : rBoarder+1] {
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

	mu.Lock()
	list, ok := lists.Load(args[0])

	if !ok {
		mu.Unlock()
		_, err := c.Conn.Write([]byte(":0\r\n"))
		return err
	}

	l, ok := list.([]any)
	if !ok {
		mu.Unlock()
		return fmt.Errorf("LLEN: list type mismatch")
	}

	ln := len(l)
	mu.Unlock()

	_, err := c.Conn.Write([]byte(":" + strconv.Itoa(ln) + "\r\n"))
	return err
}

func (c *Conn) runLPOP(args []string) error {
	mu.Lock()
	list, ok := lists.Load(args[0])

	// if doesn't exist or empty, return null string
	if !ok {
		mu.Unlock()
		_, err := c.Conn.Write([]byte("$-1\r\n"))
		return err
	}

	l, ok := list.([]any)
	if !ok {
		mu.Unlock()
		return fmt.Errorf("LPOP: list type mismatch")
	}
	if len(l) == 0 {
		mu.Unlock()
		_, err := c.Conn.Write([]byte("$-1\r\n"))
		return err
	}
	// make a copy
	cp := append([]any(nil), l...)

	var (
		toPop = 1
		err   error
	)
	if len(args) > 1 {
		toPop, err = strconv.Atoi(args[1])
		if err != nil {
			mu.Unlock()
			// handle error
			return err
		}
	}
	if toPop > len(cp) {
		toPop = len(cp)
	}

	// pop from left

	// use a slice to record what we popped to backward net I/O
	var popped []string

	for i := 0; i < toPop; i++ {
		//_, err = c.Conn.Write([]byte(serialize(cp[0].(string))))
		//if err != nil {
		// handle error
		//	return err
		//}
		popped = append(popped, cp[0].(string))
		cp = cp[1:]

	}
	lists.Store(args[0], cp)
	ch := getCh(args[0])
	mu.Unlock()

	if len(cp) == 0 {
		select {
		case <-ch:
		default:
		}
	}
	// if len(args) > 1 we need to write array instead of bulk string
	if len(args) > 1 {
		_, err = c.Conn.Write([]byte("*" + strconv.Itoa(toPop) + "\r\n"))
		if err != nil {
			// handle error
			return err
		}
	}

	for _, p := range popped {
		_, err = c.Conn.Write([]byte(serialize(p)))
		if err != nil {
			// handle error
			return err
		}
	}

	return nil
}

func (c *Conn) runBLPOP(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("BLPOP: argument count mismatch")
	}

	timeout, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		// handle error
		return err
	}
	const EPS = 1e-6
	if timeout < EPS {
		// indefinite timeout
		timeout = math.MaxInt32
	}

retryOnEmpty:
	mu.Lock()
	cp, err := getCopy(args[0])
	if err != nil {
		// handle error
		mu.Unlock()
		return err
	}

	ch := getCh(args[0])
	if len(cp) != 0 {
		// initial check passed
		// ignore select block and do popping
		toPop := cp[0].(string)
		cp = cp[1:]
		lists.Store(args[0], cp)
		mu.Unlock()
		if len(cp) == 0 {
			// set channel
			select {
			case <-ch:
			default:
			}
		}
		_, err = c.Conn.Write([]byte("*2\r\n"))
		if err != nil {
			// handle error
			return err
		}
		_, err = c.Conn.Write([]byte(serialize(args[0]) + serialize(toPop)))
		return err
	}
	mu.Unlock()

	select {
	case <-ch:
		mu.Lock()
		cp, err = getCopy(args[0])
		if err != nil {
			// handle error
			return err
		}
		// check race condition: if some goroutines pop first
		if len(cp) == 0 {
			mu.Unlock()
			goto retryOnEmpty
		}
		// pop one element
		toPop := cp[0]
		cp = cp[1:]
		// store back the list after popping
		lists.Store(args[0], cp)
		mu.Unlock()
		// check if cp is still not empty after popping
		if len(cp) != 0 {
			// if so, fill the channel for further popping
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		// encode the RESP array
		_, err = c.Conn.Write([]byte("*2\r\n"))
		if err != nil {
			// handle error
			return err
		}
		_, err = c.Conn.Write([]byte(serialize(args[0]) + serialize(toPop.(string))))
		if err != nil {
			// handle error
			return err
		}
	case <-time.After(time.Duration(1000*timeout) * time.Millisecond):
		// timeout, return null array
		_, err = c.Conn.Write([]byte("*-1\r\n"))
		if err != nil {
			// handle error
			return err
		}
	}

	return nil
}

func (c *Conn) runTYPE(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("TYPE: argument count mismatch")
	}

	mu.Lock()

	// check if args[0] is of type string
	_, ok := variables.Load(args[0])
	if ok {
		mu.Unlock()
		_, err := c.Conn.Write([]byte("+string\r\n"))
		return err
	}

	// check if args[0] is of type stream
	_, ok = streams.Load(args[0])
	if ok {
		mu.Unlock()
		_, err := c.Conn.Write([]byte("+stream\r\n"))
		return err
	}

	mu.Unlock()
	// doesn't exist
	_, err := c.Conn.Write([]byte("+none\r\n"))

	return err
}

// two types of invalid ID and other faults
const (
	TIME_NO_MISMATCH = iota
	INVALID_NO       // 0-0
	SYNTAX_ERROR
	UNKNOWN_ERROR
	// SUCCESS is success code
	SUCCESS
)

// auto-complete
const (
	// no. needs to be auto-completed
	PARTIAL_AUTO = 5
	// time and no. both need auto-completion
	FULL_AUTO = 6
)

// splitID returns the timestamp and number of an ID
func splitID(id string) (tm, no int, err error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		// invalid format
		return -1, -1, fmt.Errorf("SYNTAX ERROR in ID")
	}
	tm, err = strconv.Atoi(parts[0])
	if err != nil {
		// handle error
		return -1, -1, err
	}
	no, err = strconv.Atoi(parts[1])
	if err != nil {
		// handle error
		return tm, -1, err
	}
	return tm, no, nil
}

// checkID checks an entry's id for runXADD
func checkID(id string, topElem Entry) int {
	if id == "*" {
		return FULL_AUTO
	}

	topTime, topNo, err := splitID(topElem.id)
	if err != nil {
		return UNKNOWN_ERROR
	}
	// split ID by -
	// [NOT IMPLEMENTED] only for explicit ID of format xxxx-yyyy
	tm, no, err := splitID(id)
	if err != nil {
		if strings.HasSuffix(id, "-*") && tm >= topTime {
			// needs partially auto completion
			return PARTIAL_AUTO
		} else if strings.HasSuffix(id, "-*") && tm < topTime {
			return TIME_NO_MISMATCH
		}
		// handle error
		return SYNTAX_ERROR
	}
	// id: 0-0 is invalid
	if tm == 0 && no == 0 {
		return INVALID_NO
	}

	if topElem.kv == nil {
		// `stream` is empty
		return SUCCESS
	}
	// topTime should not be larger than tm
	// if equal, topNo should not be larger than no
	if topTime < tm || topNo < no && topTime == tm {
		return SUCCESS
	}
	return TIME_NO_MISMATCH
}

func (c *Conn) runXADD(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("XADD: lacking argument(s)")
	}
	if len(args)%2 != 0 {
		return fmt.Errorf("XADD: wrong argument format")
	}

	mu.Lock()
	// record all key-value pairs
	var kvs []KV
	for i := 2; i < len(args) && i+1 < len(args); i += 2 {
		kvs = append(kvs, KV{args[i], args[i+1]})
	}

	// append these pairs to stream
	stream, ok := streams.Load(args[0])
	if !ok {
		// if the stream does not exist, create it
		streams.Store(args[0], Stream{})
		stream, _ = streams.Load(args[0])
	}

	cp, ok := stream.(Stream)
	if !ok {
		// stream is of wrong type
		mu.Unlock()
		return fmt.Errorf("XADD: stream type mismatch")
	}
	// checkID(args[1], topElem) // topElem := cp[len(cp)-1]
	var topElem Entry
	if len(cp) == 0 {
		topElem = Entry{"0-0", nil}
	} else {
		topElem = cp[len(cp)-1]
	}
	/*if checkID(args[1], topElem) == TIME_NO_MISMATCH {
		// invalid ID
		mu.Unlock()
		_, err := c.Conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
		return err
	}*/
	tm, no, err := splitID(args[1])
	if err != nil {
		// skip auto-complements
		if !strings.HasSuffix(args[1], "-*") && args[1] != "*" {
			mu.Unlock()
			// handle error
			return err
		}
	}
	switch checkID(args[1], topElem) {
	case TIME_NO_MISMATCH:
		mu.Unlock()
		_, err := c.Conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
		return err
	case INVALID_NO:
		mu.Unlock()
		_, err := c.Conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
		return err
	case UNKNOWN_ERROR:
		mu.Unlock()
		return fmt.Errorf("XADD: Unknown errors happened")
	case PARTIAL_AUTO:
		// partially auto-implement
		topTime, topNo, err := splitID(topElem.id)
		if err != nil {
			// handle error
			mu.Unlock()
			return err
		}
		if topTime == tm {
			// no = topNo + 1
			no = topNo + 1
		} else {
			no = 0
		}
	case FULL_AUTO:
		// fully auto-implement
		// tm <- current unix time (millisecond)
		tm = int(time.Now().UnixMilli())

		topTime, topNo, err := splitID(topElem.id)
		if err != nil {
			// handle error
			mu.Unlock()
			return err
		}
		if topTime == tm {
			// no = topNo + 1
			no = topNo + 1
		} else {
			no = 0
		}
	default:
	}
	id := strconv.Itoa(tm) + "-" + strconv.Itoa(no)
	cp = append(cp, Entry{id, kvs})
	streams.Store(args[0], cp)
	mu.Unlock()
	// write back the entry id
	_, err = c.Conn.Write([]byte(serialize(id)))
	return err
}

func (c *Conn) runXRANGE(args []string) error {
	// usage: XRANGE some_key left_boarder right_boarder
	if len(args) != 3 {
		return fmt.Errorf("XRANGE: argument count mismatch")
	}

	mu.Lock()
	streamRaw, ok := streams.Load(args[0])
	if !ok {
		// create a stream
		streams.Store(args[0], Stream{})
		streamRaw, _ = streams.Load(args[0])
	}
	stream, ok := streamRaw.(Stream)
	if !ok {
		return fmt.Errorf("XRANGE: stream type mismatch")
	}

	mu.Unlock()

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
			case "BLPOP":
				if c.runBLPOP(args[1:]) != nil {
					return
				}
			case "TYPE":
				if c.runTYPE(args[1:]) != nil {
					return
				}
			case "XADD":
				if c.runXADD(args[1:]) != nil {
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
		// redis uses event loop
		go handleConn(conn)
	}
}
