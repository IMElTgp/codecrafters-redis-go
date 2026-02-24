package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sort"
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

	// whether to try to wake up waiters(BLPOP)
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
		// similar to `for cond.Wait()`
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

func (c *Conn) runXADD(args []string) error {
	if len(args) < 4 {
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
		tm = time.Now().UnixMilli()

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
	id := strconv.FormatInt(tm, 10) + "-" + strconv.FormatInt(no, 10)
	cp = append(cp, Entry{id, kvs})
	streams.Store(args[0], cp)

	// signal waiters
	waitersRaw, ok := notifyXREAD.Load(args[0])
	if !ok {
		notifyXREAD.Store(args[0], []Waiter{})
		waitersRaw, _ = notifyXREAD.Load(args[0])
	}
	waiters, ok := waitersRaw.([]Waiter)
	if !ok {
		mu.Unlock()
		return fmt.Errorf("XADD: waiter list type mismatch")
	}
	// lazy delete
	toDel := make([]bool, len(waiters))
	for i := range waiters {
		if cmpID(waiters[i].lastID, id) < 0 {
			// new ID on arrival
			// signal it and remove this record
			select {
			case waiters[i].ch <- struct{}{}:
			default:
			}
			toDel[i] = true
		}
	}
	i := 0
	for _, del := range toDel {
		if del {
			if i == 0 {
				waiters = waiters[1:]
			} else if i < len(waiters)-1 {
				waiters = append(waiters[:i], waiters[i+1:]...)
			} else {
				waiters = waiters[:len(waiters)-1]
			}
		} else {
			i++
		}
	}
	// writeback
	notifyXREAD.Store(args[0], waiters)
	mu.Unlock()
	// write back the entry id
	_, err = c.Conn.Write([]byte(serialize(id)))
	return err
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
		mu.Unlock()
		return fmt.Errorf("XRANGE: stream type mismatch")
	}
	startID, endID := args[1], args[2]
	// process optional sequence numbers
	if !strings.Contains(args[1], "-") {
		startID += "-0"
	}
	if !strings.Contains(args[1], "-") {
		// endID may be "+", which means the upper bound is the very end of the stream
		endID += "-" + strconv.FormatInt(math.MaxInt64, 10)
	}
	if startID == "-" {
		// "-" means the lower bound is the very beginning of the stream
		startID = "0-1"
	}
	// binary search the bounds
	lo := sort.Search(len(stream), func(i int) bool {
		return cmpID(stream[i].id, startID) >= 0
	})
	hi := sort.Search(len(stream), func(i int) bool {
		return cmpID(stream[i].id, endID) > 0
	})
	// slice contains all entries to be written
	slice := stream[lo:hi]
	sliceLen := len(slice)
	mu.Unlock()
	// encode a RESP array
	_, err := c.Conn.Write([]byte("*" + strconv.Itoa(sliceLen) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}
	// traverse all entries to write
	for _, e := range slice {
		_, err = c.Conn.Write([]byte("*2\r\n" + serialize(e.id)))
		if err != nil {
			// handle error
			return err
		}
		_, err = c.Conn.Write([]byte("*" + strconv.Itoa(2*len(e.kv)) + "\r\n"))
		if err != nil {
			// handle error
			return err
		}
		for _, kv := range e.kv {
			_, err = c.Conn.Write([]byte(serialize(kv.key) + serialize(kv.value)))
			if err != nil {
				// handle error
				return err
			}
		}
	}

	return nil
}

func (c *Conn) runXREAD(args []string) error {
	// if len(args) != 2 {
	// 	return fmt.Errorf("XREAD: argument count mismatch")
	//}
	// for multiple queries
	// format: STREAM <key1> <key2> ... <id1> <id2> ...
	// key1 <-> id1, key2 <-> id2, ...
	queries := [][]string{}
	block := false
	var blockTimeout int64
	var err error

	if strings.ToUpper(args[0]) == "STREAMS" {
		for i := 1; i < (len(args)+1)/2; i++ {
			queries = append(queries, []string{args[i], args[i+(len(args)-1)/2]})
		}
	} else {
		// blocking XREAD isn't compatible for multiple queries
		queries = append(queries, []string{args[3], args[4]})
		// fetch blocking timeout
		blockTimeout, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			// handle error
			return err
		}
		if blockTimeout == 0 {
			// indefinite blocking
			blockTimeout = math.MaxInt32
		}
		block = true
	}

	if !block {
		// DO NOT ADD THIS PREFIX UNTIL WE KNOW WHETHER THE RESPONSE STRING SHOULD BE NULL
		// or null array will be arrayed ([*-1\r\n])
		_, err = c.Conn.Write([]byte("*" + strconv.Itoa(len(queries)) + "\r\n"))
		if err != nil {
			// handle error
			return err
		}
	}

	for _, q := range queries {
		mu.Lock()
		streamRaw, ok := streams.Load(q[0])
		if !ok {
			streams.Store(q[0], Stream{})
			streamRaw, _ = streams.Load(q[0])
		}

		stream, ok := streamRaw.(Stream)
		if !ok {
			mu.Unlock()
			return fmt.Errorf("XREAD: stream type mismatch")
		}

		if q[1] == "$" {
			// considered as the maximum id currently available
			q[1] = stream[len(stream)-1].id
		}

		lo := sort.Search(len(stream), func(i int) bool {
			return cmpID(stream[i].id, q[1]) > 0
		})

		if lo == len(stream) {
			// lo not found
			if !block {
				// do nothing
				goto normal
			}
			waitersRaw, ok := notifyXREAD.Load(q[0])
			if !ok {
				notifyXREAD.Store(q[0], []Waiter{})
				waitersRaw, _ = notifyXREAD.Load(q[0])
			}
			waiters, ok := waitersRaw.([]Waiter)
			if !ok {
				mu.Unlock()
				return fmt.Errorf("XREAD: waiter list type mismatch")
			}
			ch := make(chan struct{}, 1)
			waiters = append(waiters, Waiter{q[1], ch})
			notifyXREAD.Store(q[0], waiters)
			// SELECT BLOCK SHOULDN'T HOLD LOCK
			mu.Unlock()
			select {
			case <-ch:
				mu.Lock()
				// FUCKING REMEMBER THAT STREAM SHOULD BE UPDATED
				streamRaw, ok = streams.Load(q[0])
				if !ok {
					mu.Unlock()
					return fmt.Errorf("GO EAT SHIT")
				}
				stream, ok = streamRaw.(Stream)
				if !ok {
					mu.Unlock()
					return fmt.Errorf("GO EAT SHIT")
				}
				// update the begining index, don't let it be sort.Search's error code as we will fallthrough to normal path
				lo = sort.Search(len(stream), func(i int) bool {
					return cmpID(stream[i].id, q[1]) > 0
				})
				goto normal
			case <-time.After(time.Duration(blockTimeout) * time.Millisecond):
				// timeout, return a null array
				_, err = c.Conn.Write([]byte("*-1\r\n"))
				return err
			}
		}
	normal:
		if block {
			// if we are sure we wouldn't return null array, add the prefix
			_, err = c.Conn.Write([]byte("*" + strconv.Itoa(len(queries)) + "\r\n"))
			if err != nil {
				// handle error
				return err
			}
		}

		slice := stream[lo:]
		mu.Unlock()

		// encode a RESP response
		// 0. prefix
		_, err = c.Conn.Write([]byte("*2\r\n"))
		if err != nil {
			// handle error
			return err
		}
		// 1. the stream key, as a bulk string
		_, err = c.Conn.Write([]byte(serialize(q[0])))
		if err != nil {
			// handle error
			return err
		}
		// 2. length of slice (entries to be written)
		_, err = c.Conn.Write([]byte("*" + strconv.Itoa(len(slice)) + "\r\n"))
		if err != nil {
			// handle error
			return err
		}
		// 3. traverse all entries
		for _, e := range slice {
			// 4. 2 for ID + KV pair slice
			// 5. id
			resp := "*2\r\n" + serialize(e.id)

			// 6. traverse all KV pairs
			resp += "*" + strconv.Itoa(2*len(e.kv)) + "\r\n"
			for _, kv := range e.kv {
				resp += serialize(kv.key) + serialize(kv.value)
			}

			_, err = c.Conn.Write([]byte(resp))
			if err != nil {
				// handle error
				return err
			}
		}
	}

	return nil
}

func (c *Conn) runINCR(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("INCR: argument count mismatch")
	}

	mu.Lock()
	// increment the value of a key by 1
	valRaw, ok := variables.Load(args[0])
	if !ok {
		// not exist
		// store back a nil value
		variables.Store(args[0], Value{"0", time.Now().Add(time.Duration(math.MaxInt32) * time.Second)})
		valRaw, _ = variables.Load(args[0])
	}
	val, ok := valRaw.(Value)
	if !ok {
		mu.Unlock()
		return fmt.Errorf("INCR: value type mismatch")
	}

	valNum, err := strconv.Atoi(val.Val)
	if err != nil {
		// handle error
		// key exists but doesn't have a numeric value
		// return an error
		mu.Unlock()
		_, err = c.Conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
		return err
	}

	variables.Store(args[0], Value{strconv.Itoa(valNum + 1), val.Ex})

	mu.Unlock()

	// write
	_, err = c.Conn.Write([]byte(":" + strconv.Itoa(valNum+1) + "\r\n"))
	if err != nil {
		// handle error
		return err
	}

	return nil
}

func (c *Conn) runMULTI(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("MULTI: this command does not support additional arguments")
	}

	_, err := c.Conn.Write([]byte("+OK\r\n"))
	return err
}

func (c *Conn) runINFO(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("INFO: argument count mismatch")
	}

	// default role: master
	role := "master"
	if !serverRole {
		role = "slave"
	}

	_, err := c.Conn.Write([]byte(serialize("role:" + role + "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" + "master_repl_offset:0")))
	return err
}

// a RESP argument parser
func parseArgs(msg string) (args []string, consumed int, err error) {
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

func (c *Conn) processMULTI(q [][]string, args []string) ([][]string, error) {
	q = append(q, args)
	_, err := c.Conn.Write([]byte("+QUEUED\r\n"))
	if err != nil {
		// handle error
		return nil, err
	}
	return q, nil
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

	// cross-command variables should be kept outside the for loop
	// MULTI blocking
	multi := false
	cmdQueue := [][]string{}
	// exec mode
	exec := false

	for {
		// for multiple commands in a line, which I haven't seen in test cases
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

		// this for loop is for a single (line of) command outside exec mode
		// so the loop condition shall contain exec when cmdQueue is not 0
		// in case a blocked command makes len(cmdQueue) > 0 and block the next command's arrival
		for totalConsumed < len(buffer[:n]) || len(cmdQueue) > 0 && exec {
			var (
				args     []string
				consumed int
			)
			// if cmdQueue is not empty and exec = true, execute commands
			if exec {
				if len(cmdQueue) == 0 {
					// quit exec mode
					exec = false
					continue
				} else {
					// process a queued command and remove it from cmdQueue
					args = cmdQueue[0]
					cmdQueue = cmdQueue[1:]
				}
			} else {
				args, consumed, err = parseArgs(string(buffer[totalConsumed:n]))
				if err != nil {
					// handle error
					return
				}
				if consumed == 0 {
					break
				}
			}
			if len(args) == 0 {
				return
			}
			switch strings.ToUpper(args[0]) {
			case "PING":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runPING() != nil {
					// handle error
					return
				}
			case "ECHO":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runECHO(args[1:]) != nil {
					// handle error
					return
				}
			case "SET":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runSET(args[1:]) != nil {
					// handle error
					return
				}
			case "GET":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runGET(args[1:]) != nil {
					// handle error
					return
				}
			case "RPUSH":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runRPUSH(args[1:]) != nil {
					return
				}
			case "LPUSH":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runLPUSH(args[1:]) != nil {
					return
				}
			case "LRANGE":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runLRANGE(args[1:]) != nil {
					return
				}
			case "LLEN":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runLLEN(args[1:]) != nil {
					return
				}
			case "LPOP":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runLPOP(args[1:]) != nil {
					return
				}
			case "BLPOP":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runBLPOP(args[1:]) != nil {
					return
				}
			case "TYPE":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runTYPE(args[1:]) != nil {
					return
				}
			case "XADD":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runXADD(args[1:]) != nil {
					return
				}
			case "XRANGE":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runXRANGE(args[1:]) != nil {
					return
				}
			case "XREAD":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runXREAD(args[1:]) != nil {
					return
				}
			case "INCR":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runINCR(args[1:]) != nil {
					return
				}
			case "MULTI":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runMULTI(args[1:]) != nil {
					return
				}
				multi = true
			case "EXEC":
				if !multi {
					// haven't called MULTI
					_, _ = c.Conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
					return
				}
				multi = false
				exec = true
				// remember to update totalConsumed here or EXEC may be considered as a command inside cmdQueue
				totalConsumed += consumed
				_, err = c.Conn.Write([]byte("*" + strconv.Itoa(len(cmdQueue)) + "\r\n"))
				if err != nil {
					// handle error
					return
				}
			case "DISCARD":
				if !multi {
					// haven't called MULTI
					_, _ = c.Conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
					return
				}
				multi = false
				// discard cmdQueue
				cmdQueue = [][]string{}
				_, err = c.Conn.Write([]byte("+OK\r\n"))
				if err != nil {
					// handle error
					return
				}
			case "INFO":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runINFO(args[1:]) != nil {
					return
				}
			}
		skip:
			if !exec {
				// if exec, consumed has been counted
				totalConsumed += consumed
			}
		}

	}
}

// parse CLI arguments
func parseCLIArgs(args []string) (int, Config) {
	port := flag.Int("port", 6379, "server port")
	replicaof := flag.String("replicaof", "master 6379", "replication of this server")
	flag.Parse()

	hostAndPort := strings.Split(*replicaof, " ")
	masHost, masPort := hostAndPort[0], hostAndPort[1]

	return *port, Config{masHost, masPort}
}

func sendHandshake(masterHost, masterPort string) error {
	if serverRole {
		return nil
	}

	masterConn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		// handle error
		return err
	}

	// replica sends a PING to the master
	// replica sends REPLCONF twice and PSYNC
	_, err = masterConn.Write([]byte("*1\r\n" + serialize("PING") + "*2\r\n" + serialize("REPLCONF") + serialize("REPLCONF") + "*1\r\n" + serialize("PSYNC")))
	return err
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage

	port, config := parseCLIArgs(os.Args)

	if config.port != strconv.Itoa(port) {
		// this server's master is not itself, which indicates that it is the slave
		serverRole = false
	}

	if sendHandshake(config.host, config.port) != nil {
		return
	}

	address := "0.0.0.0:" + strconv.Itoa(port)

	l, err := net.Listen("tcp", address)
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
