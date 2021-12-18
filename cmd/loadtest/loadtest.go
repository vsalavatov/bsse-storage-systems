package main

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"github.com/vsalavatov/bsse-storage-systems/protocol"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
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
	Put   bool
	Get   bool
	Count int
	Port  int
}

func printUsageAndExit() {
	fmt.Println("Usage: ", os.Args[0], " [put] [get] <count> <port=4242>")
	os.Exit(1)
}

func parseArgs() {
	i := 1
	if len(os.Args) == i {
		printUsageAndExit()
	}
	for i < len(os.Args) {
		if os.Args[i] == "put" {
			Options.Put = true
		} else if os.Args[i] == "get" {
			Options.Get = true
		} else {
			break
		}
		i += 1
	}
	if i == len(os.Args) {
		printUsageAndExit()
	}
	count, err := strconv.Atoi(os.Args[i])
	if err != nil {
		printUsageAndExit()
	}
	Options.Count = count
	i += 1
	if i < len(os.Args) {
		port, err := strconv.Atoi(os.Args[i])
		if err != nil {
			printUsageAndExit()
		}
		Options.Port = port
	} else {
		Options.Port = 4242
	}
	if !Options.Put && !Options.Get {
		Options.Put = true
		Options.Get = true
	}
}

func makeValue(key string) []byte {
	hs := murmur3.New64()
	_, _ = hs.Write([]byte(key))
	rnd := rand.New(rand.NewSource(int64(hs.Sum64())))
	size := 1
	nextSize := 1
	for nextSize < 2*1024*1024 { // up to 2 MiB
		size = nextSize
		r := rnd.Uint32() % 100
		if r <= 69 {
			nextSize = int(float32(size) * (1.5 + rnd.Float32()))
		} else {
			break
		}
	}
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = byte(uint8(size+i+i*i+i*i*i)%26 + uint8('a'))
	}
	return value
}

func main() {
	parseArgs()

	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	go func() {
		<-sigchan
		fmt.Println("Got interrupt. Shutting down...")
		cancel()
	}()

	threads := 4
	if threads > Options.Count {
		threads = 1
	}

	var wg sync.WaitGroup
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", fmt.Sprint("127.0.0.1:", Options.Port))
			if err != nil {
				fmt.Println("failed to connect to the server: ", err.Error())
				os.Exit(2)
			}
			defer conn.Close()

			count := (Options.Count + threads - 1) / threads // per thread

			reqIdToSize := make(map[int]int)
			totalValueSize := uint64(0)
			defer func() {
				kek := float64(totalValueSize)
				level := 0
				for kek > 1024 {
					kek /= 1024.0
					level += 1
				}
				fmt.Println("thread", id, "stored", kek, []string{"bytes", "KiB", "MiB", "GiB", "PiB"}[level])
			}()

			var lwg sync.WaitGroup
			lwg.Add(2)
			writer := func() {
				defer lwg.Done()
				if Options.Put {
					for i := 0; i < count; i++ {
						select {
						case <-ctx.Done():
							break
						default:
							key := fmt.Sprint(id, "-req-", i)
							value := makeValue(key)
							totalValueSize += uint64(len(value))
							reqIdToSize[i] = len(value)
							putReq := protocol.TPutRequest{RequestId: uint64(i), Key: key, Value: []byte(value)}
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
						}
					}
				} else {
					for i := 0; i < count; i++ {
						select {
						case <-ctx.Done():
							break
						default:
							key := fmt.Sprint(id, "-req-", i)
							value := makeValue(key)
							reqIdToSize[i] = len(value)
						}
					}
				}
				if Options.Get {
					for i := 0; i < count; i++ {
						select {
						case <-ctx.Done():
							break
						default:
							key := fmt.Sprint(id, "-req-", i)
							getReq := protocol.TGetRequest{RequestId: uint64(i), Key: key}
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
						}
					}
				}
			}
			reader := func() {
				defer lwg.Done()
				requestsCoef := 0
				if Options.Get {
					requestsCoef += 1
				}
				if Options.Put {
					requestsCoef += 1
				}
				for i := 0; i < count*requestsCoef; i++ {
					select {
					case <-ctx.Done():
						break
					default:
						msg, err := readMessage(conn)
						if err != nil {
							fmt.Println("failed to read get response: ", err.Error())
							return
						}
						switch msg.msgType {
						case GET_RESPONSE:
							getResp := protocol.TGetResponse{}
							err = proto.Unmarshal(msg.data[:], &getResp)
							if err != nil {
								fmt.Println("failed to unmarshal get response: ", err.Error())
								return
							}
							if len(getResp.Value) != reqIdToSize[int(getResp.RequestId)] {
								fmt.Println("WRONG: thread #", id, " reqId=", getResp.RequestId, " expected len ", reqIdToSize[int(getResp.RequestId)], " got len ", len(getResp.Value))
								return
							}
							for j := 0; j < len(getResp.Value); j++ {
								expected := byte(uint8(len(getResp.Value)+j+j*j+j*j*j)%26 + uint8('a'))
								if getResp.Value[j] != expected {
									fmt.Println("WRONG: thread #", id, " reqId=", getResp.RequestId, " at ", j+1, " position: expected sym ", expected, " got sym ", getResp.Value[j])
									return
								}
							}
						case PUT_RESPONSE:
							putResp := protocol.TPutResponse{}
							err = proto.Unmarshal(msg.data[:], &putResp)
							if err != nil {
								fmt.Println("failed to unmarshal put response: ", err.Error())
								return
							}
							// skip
						default:
							fmt.Println("unexpected message type: got ", msg.msgType)
							return
						}
					}
				}
			}
			go writer()
			go reader()
			lwg.Wait()
		}(i)
	}

	wg.Wait()
}
