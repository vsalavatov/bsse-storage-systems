package main

import (
	"fmt"
	"net"
	"time"
)

type Message struct {
	msgType byte
	data    []byte
}

func readMessage(conn net.Conn) (Message, error) {
	var header [5]byte
	headerPos := 0
	for headerPos < 5 {
		n, err := conn.Read(header[headerPos:5])
		if err != nil {
			return Message{}, err
		}
		headerPos += n
	}
	err := conn.SetReadDeadline(time.Now().Add(time.Hour * 24 * 365))
	if err != nil {
		return Message{}, err
	}
	length := uint32(0)
	for i := 0; i < 4; i++ {
		length |= uint32(header[i+1]) << (i * 8)
	}
	if length > kMaxMessageSize {
		return Message{}, fmt.Errorf("message value is too big")
	}
	buf := make([]byte, length)
	toRead := length
	for toRead > 0 {
		n, err := conn.Read(buf[length-toRead : length])
		if err != nil {
			return Message{}, err
		}
		toRead -= uint32(n)
	}
	return Message{
		msgType: header[0],
		data:    buf,
	}, nil
}

func writeMessage(conn net.Conn, msg Message) error {
	l := uint32(len(msg.data))
	header := [5]byte{msg.msgType}
	for i := 0; i < 4; i++ {
		header[i+1] = byte((l >> (i * 8)) & 0xff)
	}
	_, err := conn.Write(append(header[:], msg.data...))
	return err
}
