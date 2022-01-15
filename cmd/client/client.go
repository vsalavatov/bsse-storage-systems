package main

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/vsalavatov/bsse-storage-systems/protocol"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	PUT_REQUEST  = 1
	PUT_RESPONSE = 2
	GET_REQUEST  = 3
	GET_RESPONSE = 4
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

var Options struct {
	Port int
}

func printUsageAndExit() {
	fmt.Println("Usage: ", os.Args[0], " <port=4242>")
	os.Exit(1)
}

func parseArgs() {
	if len(os.Args) == 1 {
		Options.Port = 4242
		return
	}
	if len(os.Args) > 2 {
		printUsageAndExit()
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		printUsageAndExit()
	}
	Options.Port = port
}

func main() {
	parseArgs()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("usage:\n1. get request:\n<key>\n2. put request:\n<key>=<value>\n")

	conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", Options.Port))
	if err != nil {
		fmt.Println("failed to connect to the server: ", err.Error())
		os.Exit(2)
	}
	defer conn.Close()
	reqId := 0
	for {
		reqId += 1
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		input = input[:len(input)-1] // trim \n
		if err != nil {
			break
		}
		equalsPos := 0
		for equalsPos < len(input) {
			if input[equalsPos] == '=' {
				break
			}
			equalsPos += 1
		}
		if equalsPos == len(input) { // get
			getReq := protocol.TGetRequest{RequestId: uint64(reqId), Key: input}
			buf, err := proto.Marshal(&getReq)
			if err != nil {
				fmt.Println("failed to marshal get request: ", err.Error())
				return
			}
			err = writeMessage(conn, Message{
				msgType: GET_REQUEST,
				data:    buf,
			})
			if err != nil {
				fmt.Println("failed to send get request: ", err.Error())
				return
			}
			msg, err := readMessage(conn)
			if err != nil {
				fmt.Println("failed to read get response: ", err.Error())
				return
			}
			if msg.msgType != GET_RESPONSE {
				fmt.Println("mismatched message type: expected GET_RESPONSE, got ", msg.msgType)
				return
			}
			getResp := protocol.TGetResponse{}
			err = proto.Unmarshal(msg.data[:], &getResp)
			if err != nil {
				fmt.Println("failed to unmarshal get response: ", err.Error())
				return
			}
			fmt.Println(getResp.RequestId, "#", input, "=", string(getResp.Value))
		} else { // put
			key := input[:equalsPos]
			value := input[equalsPos+1:]
			putReq := protocol.TPutRequest{RequestId: uint64(reqId), Key: key, Value: []byte(value)}
			buf, err := proto.Marshal(&putReq)
			if err != nil {
				fmt.Println("failed to marshal put request: ", err.Error())
				return
			}
			err = writeMessage(conn, Message{
				msgType: PUT_REQUEST,
				data:    buf,
			})
			if err != nil {
				fmt.Println("failed to send put request: ", err.Error())
				return
			}
			msg, err := readMessage(conn)
			if err != nil {
				fmt.Println("failed to read put response: ", err.Error())
				return
			}
			if msg.msgType != PUT_RESPONSE {
				fmt.Println("mismatched message type: expected PUT_RESPONSE, got ", msg.msgType)
				return
			}
			putResp := protocol.TPutResponse{}
			err = proto.Unmarshal(msg.data[:], &putResp)
			if err != nil {
				fmt.Println("failed to unmarshal put response: ", err.Error())
				return
			}
			fmt.Println(putResp.RequestId, "# ok")
		}
	}
}
