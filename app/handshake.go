package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

/**
 * handshake.go
 * handles handshake that happens between replica and master
 * one PING, two REPLCONF and one PSYNC
 */

func sendPING(masterHost, masterPort string, c *Conn) error {
	if serverRole {
		return nil
	}

	_, err := c.Conn.Write([]byte("*1\r\n" + serialize("PING")))
	return err

}

// sendREPLCONF1 is responsible for sending listening-port
func sendREPLCONF1(masterPort string, c *Conn) error {
	if serverRole {
		return nil
	}

	_, err := c.Conn.Write([]byte("*3\r\n" + serialize("REPLCONF") + serialize("listening-port") + serialize(masterPort)))
	return err
}

// sendREPLCONF2 is responsible for sending capa and npsync2 (hard coded so far)
func sendREPLCONF2(c *Conn) error {
	if serverRole {
		return nil
	}

	_, err := c.Conn.Write([]byte("*3\r\n" + serialize("REPLCONF") + serialize("capa") + serialize("npsync2")))
	return err
}

func sendPSYNC(c *Conn) error {
	if serverRole {
		return nil
	}

	_, err := c.Conn.Write([]byte("*3\r\n" + serialize("PSYNC") + serialize("?") + serialize("-1")))
	return err
}

func readFromMaster(c *Conn) error {
	if serverRole {
		return nil
	}
	kind, line, err := readRESPValue(c.Conn)
	if err != nil {
		return err
	}
	// After PSYNC, master sends: +FULLRESYNC ...\r\n then an RDB bulk string.
	if kind == '+' && strings.HasPrefix(line, "FULLRESYNC") {
		_, _, err = readRESPValue(c.Conn)
		if err != nil {
			return err
		}
	}
	return nil
}

func readRESPValue(conn io.Reader) (kind byte, line string, err error) {
	b, err := readByte(conn)
	if err != nil {
		return 0, "", err
	}

	switch b {
	case '+', '-', ':':
		line, err = readLine(conn)
		return b, line, err
	case '$':
		line, err = readLine(conn)
		if err != nil {
			return b, "", err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return b, "", err
		}
		if n == -1 {
			return b, "", nil
		}
		// bulk string payload + \r\n
		if err := discardN(conn, n+2); err != nil {
			return b, "", err
		}
		return b, "", nil
	case '*':
		line, err = readLine(conn)
		if err != nil {
			return b, "", err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return b, "", err
		}
		if n <= 0 {
			return b, "", nil
		}
		for i := 0; i < n; i++ {
			if _, _, err = readRESPValue(conn); err != nil {
				return b, "", err
			}
		}
		return b, "", nil
	default:
		return b, "", fmt.Errorf("unexpected RESP type byte: %q", b)
	}
}

func readByte(conn io.Reader) (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(conn, buf[:])
	return buf[0], err
}

func readLine(conn io.Reader) (string, error) {
	var b strings.Builder
	for {
		ch, err := readByte(conn)
		if err != nil {
			return "", err
		}
		if ch == '\n' {
			break
		}
		b.WriteByte(ch)
	}
	line := b.String()
	return strings.TrimSuffix(line, "\r"), nil
}

func discardN(conn io.Reader, n int) error {
	if n <= 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, conn, int64(n))
	return err
}
