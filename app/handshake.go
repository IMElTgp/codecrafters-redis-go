package main

import "net"

func sendPING(masterHost, masterPort string, masterConn net.Conn) error {
	if serverRole {
		return nil
	}

	_, err := masterConn.Write([]byte("*1\r\n" + serialize("PING")))
	return err

}

func sendREPLCONF1(masterPort string, masterConn net.Conn) error {
	if serverRole {
		return nil
	}

	_, err := masterConn.Write([]byte("*3\r\n" + serialize("REPLCONF") + serialize("listening-port") + serialize(masterPort)))
	return err
}

func sendREPLCONF2(masterConn net.Conn) error {
	if serverRole {
		return nil
	}

	_, err := masterConn.Write([]byte("*3\r\n" + serialize("REPLCONF") + serialize("capa") + serialize("npsync2")))
	return err
}

func sendPSYNC(masterConn net.Conn) error {
	if serverRole {
		return nil
	}

	_, err := masterConn.Write([]byte("*3\r\n" + serialize("PSYNC") + serialize("?") + serialize("-1")))
	return err
}

func readFromMaster(masterConn net.Conn) error {
	if serverRole {
		return nil
	}
	buffer := make([]byte, 1024)
	_, err := masterConn.Read(buffer)
	if err != nil {
		// handle error
		return err
	}

	return nil
}
