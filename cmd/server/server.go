package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"github.com/vsalavatov/bsse-storage-systems/batch"
	"github.com/vsalavatov/bsse-storage-systems/hashtable"
	"github.com/vsalavatov/bsse-storage-systems/keyvalue"
	"github.com/vsalavatov/bsse-storage-systems/protocol"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
)

const (
	kRequestsBufferSize = 256
	kMaxMessageSize     = keyvalue.MaxValueSize + 16*1024 // 4 MiB + 16 KiB for key and header
)

var Options struct {
	port     int
	logsDir  string
	verbose  bool
	maxConns int
	//htScatterBits  int
	//kvsScatterBits int
}

var bp *batch.BatchKeyValueProcessor

const (
	PUT_REQUEST  = 1
	PUT_RESPONSE = 2
	GET_REQUEST  = 3
	GET_RESPONSE = 4
)

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
						debug("#", putReq.RequestId, " ? PUT ", putReq.Key, "=", putReq.GetValue())
					}
					var keyBuf [hashtable.KeySize]byte
					copy(keyBuf[:], putReq.Key)
					bp.Put(keyBuf, putReq.Value, func(err error) {
						if err != nil {
							logErr(conn, "kvs has failed to put ", putReq.Key, "->", string(putReq.Value), err.Error())
							return
						}
						buf, err := proto.Marshal(&protocol.TPutResponse{RequestId: putReq.RequestId})
						if err != nil {
							logErr(conn, "failed to marshal put response message:", err.Error())
							return
						}
						if Options.verbose {
							debug("#", putReq.RequestId, " ! PUT ", putReq.Key, "=", string(putReq.Value))
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
					bp.Get(keyBuf, func(value keyvalue.Value, err error) {
						if err != nil && err != hashtable.KeyNotFoundError {
							logErr(conn, "kvs has failed to get value of", getReq.Key, ":", err.Error())
							return
						}
						if Options.verbose && err == hashtable.KeyNotFoundError {
							logErr(conn, "kvs has no value for key ", getReq.Key)
						}
						buf, err := proto.Marshal(&protocol.TGetResponse{RequestId: getReq.RequestId, Value: value})
						if err != nil {
							logErr(conn, "failed to marshal get response message:", err.Error())
							return
						}
						if Options.verbose {
							debug("#", getReq.RequestId, " ! GET ", getReq.Key, " value=", string(value))
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
	//htScatterBits := flag.Int("ht-scatter-bits", 1, "amount of hash bits used to scatter keys between hashtables")
	//kvsScatterBits := flag.Int("kvs-scatter-bits", 3, "amount of hash bits used to scatter keys between data storages")

	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		os.Exit(1)
	}
	Options.port = *port
	Options.logsDir = *logsDir
	Options.verbose = *verbose
	Options.maxConns = *maxConns
	//Options.htScatterBits = *htScatterBits
	//Options.kvsScatterBits = *kvsScatterBits
}

func main() {
	parseArgs()

	err := os.MkdirAll(Options.logsDir, 0744)
	if err != nil {
		println("failed to create logs dir: ", err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	kvs := keyvalue.NewPersistentKeyValueStorage(Options.logsDir, func(key hashtable.Key) uint64 {
		h := murmur3.New64()
		_, _ = h.Write(key[:])
		result := h.Sum64()
		if Options.verbose {
			debug("kvs-hash(", key, "@", string(key[:]), ")=", result)
		}
		return result
	})
	if err = kvs.Restore(); err != nil {
		fmt.Println("failed to initialize kvs: ", err.Error())
		return
	}

	bp = batch.NewBatchKeyValueProcessor(ctx, kvs)

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
