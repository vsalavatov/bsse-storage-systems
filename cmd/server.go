package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"github.com/vsalavatov/bsse-storage-systems/hashtable"
	"github.com/vsalavatov/bsse-storage-systems/protocol"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

const (
	kRequestsBufferSize = 4096
)

var Options struct {
	port        int
	logsDir     string
	verbose     bool
	maxConns    int
	scatterBits int
}

var ht hashtable.PersistentHashTable

const (
	PUT_REQUEST  = 1
	PUT_RESPONSE = 2
	GET_REQUEST  = 3
	GET_RESPONSE = 4
)

func debug(a ...interface{}) {
	fmt.Println("[DEBUG] ", a)
}

func logErr(conn net.Conn, args ...interface{}) {
	fmt.Println("failed to service ", conn.RemoteAddr().String(), ": ", args)
}

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

func handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	responses := make(chan Message, kRequestsBufferSize)

	reader := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				if err != nil {
					logErr(conn, err.Error())
					return
				}
				msg, err := readMessage(conn)
				if err != nil {
					if err == io.EOF {
						return
					}
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue
					}
					logErr(conn, err.Error())
					return
				}
				switch msg.msgType {
				case PUT_REQUEST:
					putReq := protocol.TPutRequest{}
					err := proto.Unmarshal(msg.data[:], &putReq)
					if err != nil {
						logErr(conn, "failed to unmarshal put request message: ", err.Error())
						return
					}
					if Options.verbose {
						debug("#", putReq.RequestId, " ? PUT ", putReq.Key, "=", putReq.Offset)
					}
					var keyBuf [hashtable.KeySize]byte
					copy(keyBuf[:], putReq.Key)
					ht.Put(keyBuf, putReq.Offset, func(err error) {
						if err != nil {
							logErr(conn, "hash table has failed to put ", putReq.Key, "->", strconv.FormatUint(putReq.Offset, 10), err.Error())
							return
						}
						buf, err := proto.Marshal(&protocol.TPutResponse{RequestId: putReq.RequestId})
						if err != nil {
							logErr(conn, "failed to marshal put response message:", err.Error())
							return
						}
						if Options.verbose {
							debug("#", putReq.RequestId, " ! PUT ", putReq.Key, "=", putReq.Offset)
						}
						responses <- Message{msgType: PUT_RESPONSE, data: buf}
					})
				case GET_REQUEST:
					getReq := protocol.TGetRequest{}
					err := proto.Unmarshal(msg.data[:], &getReq)
					if err != nil {
						logErr(conn, "failed to unmarshal get request message: ", err.Error())
						return
					}
					if Options.verbose {
						debug("#", getReq.RequestId, " ? GET ", getReq.Key)
					}
					var keyBuf [hashtable.KeySize]byte
					copy(keyBuf[:], getReq.Key)
					ht.Get(keyBuf, func(offset hashtable.Offset, err error) {
						if err != nil && err != hashtable.KeyNotFoundError {
							logErr(conn, "hash table has failed to get offset of", getReq.Key, ":", err.Error())
							return
						}
						if Options.verbose && err == hashtable.KeyNotFoundError {
							logErr(conn, "hash table has no offset for key ", getReq.Key)
						}
						buf, err := proto.Marshal(&protocol.TGetResponse{RequestId: getReq.RequestId, Offset: offset})
						if err != nil {
							logErr(conn, "failed to marshal get response message:", err.Error())
							return
						}
						if Options.verbose {
							debug("#", getReq.RequestId, " ! GET ", getReq.Key, " offset=", offset)
						}
						responses <- Message{GET_RESPONSE, buf}
					})
				default:
					logErr(conn, "wrong message type: ", string(msg.msgType))
				}
			}
		}
	}
	writer := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-responses:
				err := writeMessage(conn, msg)
				if err != nil {
					logErr(conn, "failed to send put response message:", err.Error())
					return
				}
			}
		}
	}
	go reader()
	go writer()
	wg.Wait()
}

func handleConnections(ctx context.Context, wg *sync.WaitGroup, queue <-chan net.Conn) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case conn := <-queue:
			handleConnection(ctx, conn)
		}
	}
}

func parseArgs() {
	port := flag.Int("port", 4242, "the port server will listen on")
	logsDir := flag.String("logs", "data", "path to the folder where data is located")
	verbose := flag.Bool("verbose", false, "print logs")
	maxConns := flag.Int("max-conns", 32, "maximum concurrent connections to handle")
	scatterBits := flag.Int("scatter-bits", 6, "amount of hash bits used to scatter keys between hashtables")
	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		os.Exit(1)
	}
	Options.port = *port
	Options.logsDir = *logsDir
	Options.verbose = *verbose
	Options.maxConns = *maxConns
	Options.scatterBits = *scatterBits
}

func main() {
	parseArgs()

	err := os.MkdirAll(Options.logsDir, 0744)
	if err != nil {
		println("failed to create logs dir: ", err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	ht = hashtable.NewScatterPHT(ctx, Options.scatterBits, func(key hashtable.Key) uint64 {
		h := murmur3.New64()
		_, _ = h.Write(key[:])
		result := h.Sum64()
		if Options.verbose {
			debug("hash(", key, "@", string(key[:]), ")=", result)
		}
		return result
	}, Options.logsDir)
	if err = ht.Restore(); err != nil {
		println("failed to restore data: ", err.Error())
		return
	}
	fmt.Println("[SERVER] Restored ", ht.Size(), " elements")

	listener, err := net.Listen("tcp", fmt.Sprint("127.0.0.1:", Options.port))
	if err != nil {
		println("failed to start server:", err.Error())
		return
	}

	connsQueue := make(chan net.Conn, Options.maxConns)
	wg := sync.WaitGroup{}
	wg.Add(Options.maxConns)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	for i := 0; i < Options.maxConns; i++ {
		go handleConnections(ctx, &wg, connsQueue)
	}

	go func() {
		<-sigchan
		fmt.Println("Got interrupt. Shutting down...")
		cancel()
		listener.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			goto loop
		default:
			conn, err := listener.Accept()
			if err != nil {
				println("failed to accept a connection:", err.Error())
				return
			}
			connsQueue <- conn
		}
	}
loop:

	wg.Wait()
}
