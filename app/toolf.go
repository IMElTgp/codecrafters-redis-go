package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

// Waiter is for presenting waiters waiting for XREAD
type Waiter struct {
	lastID string        // minimal ID provided by XREAD command (exclusive)
	ch     chan struct{} // to signal this waiter
}

// Config is for <host, port> binary tuple
type Config struct {
	host string
	port string
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

// a global hash map for notifying XREAD
// string(stream name) -> []Waiter
var notifyXREAD sync.Map // Map[string]chan struct{}

// a global boolean variable to mark the server's role
// false for slave and true for master
// default: true
var serverRole = true

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

// cmpID is a simple tool function for comparing ID for binary searches
func cmpID(id1, id2 string) int {
	tm1, no1, err := splitID(id1)
	if err != nil {
		// handle error
		return -1
	}
	tm2, no2, err := splitID(id2)
	if err != nil {
		// handle error
		return -1
	}
	if tm1 < tm2 {
		return -1
	}
	if tm1 > tm2 {
		return 1
	}
	if no1 < no2 {
		return -1
	}
	if no1 > no2 {
		return 1
	}
	return 0
}

// a RESP argument parser
func parseArgs(msg string) (args []string, consumed int, err error) {
	if msg == "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n" {
		return []string{"REPLCONF", "GETACK", "*"}, len(msg), nil
	}
	if len(msg) == 0 {
		return nil, 0, fmt.Errorf("parseArgs: msg is empty")
	}
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
		if b == '\r' {
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
		if b == '*' && (msg[i+1] >= '0' && msg[i+1] <= '9') {
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

// parse CLI arguments
func parseCLIArgs(args []string) (int, Config) {
	defaultValue := ""
	for _, arg := range args {
		if arg == "--replicaof" {
			defaultValue = "master 6379"
			break
		}
	}

	port := flag.Int("port", 6379, "server port")
	replicaof := flag.String("replicaof", defaultValue, "replication of this server")
	flag.Parse()

	hostAndPort := strings.Split(*replicaof, " ")
	if len(hostAndPort) == 1 {
		return *port, Config{}
	}
	masHost, masPort := hostAndPort[0], hostAndPort[1]

	return *port, Config{masHost, masPort}
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
func splitID(id string) (tm, no int64, err error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		// invalid format
		return -1, -1, fmt.Errorf("SYNTAX ERROR in ID")
	}
	tm, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		// handle error
		return -1, -1, err
	}
	no, err = strconv.ParseInt(parts[1], 10, 64)
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
