package main

import (
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
	kind, line, err := readRESPLine(c.Conn)
	if err != nil {
		return err
	}
	// After PSYNC, master sends: +FULLRESYNC ...\r\n then an RDB bulk string
	if kind == '+' && strings.HasPrefix(line, "FULLRESYNC") {
		return readRDBBulk(c.Conn)
	}
	if kind == '$' {
		n, err := strconv.Atoi(line)
		if err != nil {
			return err
		}
		if n > 0 {
			return discardN(c.Conn, n)
		}
	}
	return nil
}

func readRESPLine(conn io.Reader) (kind byte, line string, err error) {
	// read the RESP type byte (+, -, :, $, *) before reading the rest of the line
	b, err := readByte(conn)
	if err != nil {
		return 0, "", err
	}
	// read the content of the RESP line
	line, err = readLine(conn)
	return b, line, err
}

// readRDBBulk is the special case reader for the RDB payload
func readRDBBulk(conn io.Reader) error {
	// read the $<len>\r\n header
	// b is for $
	b, err := readByte(conn)
	if err != nil {
		return err
	}
	if b != '$' {
		return io.ErrUnexpectedEOF
	}
	// read the length of RDB payload
	line, err := readLine(conn)
	if err != nil {
		return err
	}
	n, err := strconv.Atoi(line)
	if err != nil {
		return err
	}
	if n <= 0 {
		return nil
	}
	// discard the payload
	// RDB bulk payload has no trailing \r\n in replication stream
	return discardN(conn, n)
}

// readByte reads one single byte and return it
func readByte(conn io.Reader) (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(conn, buf[:])
	return buf[0], err
}

// readLine reads an entire line from conn and return this line
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

// discardN is used to skip n bytes of conn
func discardN(conn io.Reader, n int) error {
	if n <= 0 {
		return nil
	}
	// throw away (discard) n bytes from conn
	_, err := io.CopyN(io.Discard, conn, int64(n))
	return err
}
