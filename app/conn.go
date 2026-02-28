package main

import (
	"io"
	"net"
	"strconv"
	"strings"
)

// offset represents current server's total processed bytes of commands
var offset int

type Conn struct {
	Conn   net.Conn
	silent bool
}

func (c *Conn) write(p []byte) (int, error) {
	if c.silent {
		return len(p), nil
	}
	return c.Conn.Write(p)
}

// all replica connections for propagation
var replicaConns = make(map[net.Conn]bool)

// working function which carries logics related to client serving
func handleConn(c *Conn) {
	defer func() {
		err := c.Conn.Close()
		if err != nil {
			// handle error
			return
		}
	}()

	var buffer = make([]byte, 1024)

	// cross-command variables should be kept outside the for loop
	// MULTI blocking
	multi := false
	cmdQueue := [][]string{}
	// exec mode
	exec := false
	// whether the command is from replica
	fromReplica := false
	// write command list
	isWriteCmd := map[string]bool{
		"SET":   true,
		"RPUSH": true,
		"LPUSH": true,
		"LPOP":  true,
		"BLPOP": true,
		"XADD":  true,
		"INCR":  true,
	}

	for {
		// for multiple commands in a line, which I haven't seen in test cases
		n, err := c.Conn.Read(buffer)
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

			// judge if the incoming msg is from replica handshake
			if strings.ToUpper(args[0]) == "PSYNC" {
				fromReplica = true
				mu.Lock()
				replicaConns[c.Conn] = true
				mu.Unlock()
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
					if serverRole {
						_, _ = c.Conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
					}
					return
				}
				multi = false
				exec = true
				// remember to update totalConsumed here or EXEC may be considered as a command inside cmdQueue
				totalConsumed += consumed
				if serverRole {
					_, err = c.Conn.Write([]byte("*" + strconv.Itoa(len(cmdQueue)) + "\r\n"))
					if err != nil {
						// handle error
						return
					}
				}
			case "DISCARD":
				if !multi {
					// haven't called MULTI
					if serverRole {
						_, _ = c.Conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
					}
					return
				}
				multi = false
				// discard cmdQueue
				cmdQueue = [][]string{}
				if serverRole {
					_, err = c.Conn.Write([]byte("+OK\r\n"))
					if err != nil {
						// handle error
						return
					}
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
			case "REPLCONF":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runREPLCONF(args[1:]) != nil {
					return
				}
			case "PSYNC":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runPSYNC(args[1:]) != nil {
					return
				}
			case "WAIT":
				if multi {
					cmdQueue, err = c.processMULTI(cmdQueue, args)
					if err != nil {
						// handle error
						return
					}
					goto skip
				}
				if c.runWAIT(args[1:]) != nil {
					return
				}
			}
			// do propagation
			// this should NOT be put inside `skip` label
			if !fromReplica && serverRole && isWriteCmd[args[0]] {
				// propagation needs three conditions met:
				// 1. this server is master
				// 2. this command is not from replica
				// 3. this command is a write command

				// encode these arguments into a RESP array
				propagation := "*" + strconv.Itoa(len(args)) + "\r\n"
				for _, arg := range args {
					propagation += serialize(arg)
				}
				// iterate all replica connections
				mu.Lock()
				for repConn := range replicaConns {
					_, err = repConn.Write([]byte(propagation))
					if err != nil {
						mu.Unlock()
						// handle error
						return
					}
				}
				mu.Unlock()
			}
		skip:
			if !exec {
				// if exec, consumed has been counted
				totalConsumed += consumed
				mu.Lock()
				offset += consumed
				mu.Unlock()
			}
		}

	}
}
