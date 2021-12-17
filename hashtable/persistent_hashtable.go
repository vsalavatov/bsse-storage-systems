package hashtable

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
)

const kInitSize = 64
const kMaxLoadFactor = 0.75
const kScaleFactor = 1.66
const kMaxRecordsPerLogFile = 256 * 1024 * 1024 / KeyOffsetSize // about 256 MiB per log file
const kRequestsBufferSize = 512

type PersistentHashTable interface {
	HashTable
	Restore() error
}

type htNode struct {
	KeyOffset
	isOccupied bool
}

const (
	kPUT = iota
	kGET
)

type htRequest struct {
	reqType  uint8
	key      Key
	offset   Offset
	callback func(Offset, error)
}

type htResponse struct {
	offset Offset
	err    error
}

// persistentHashTable
// stores the sequence of put operations in log files in the logDir folder;
type persistentHashTable struct {
	data     []htNode
	elements uint64
	hasher   Hasher

	logDir             string
	currentFile        *os.File
	currentFileIndex   uint64
	currentFileRecords uint64

	requests chan htRequest
	ctx      context.Context
}

func NewPersistentHashTable(ctx context.Context, hasher Hasher, logDir string) PersistentHashTable {
	ht := &persistentHashTable{
		data:               make([]htNode, kInitSize),
		elements:           0,
		hasher:             hasher,
		logDir:             logDir,
		currentFile:        nil,
		currentFileIndex:   0,
		currentFileRecords: 0,
		requests:           make(chan htRequest, kRequestsBufferSize),
		ctx:                ctx,
	}
	go ht.run()
	return ht
}

func (ht *persistentHashTable) run() {
	for {
		select {
		case <-ht.ctx.Done():
			return
		case req := <-ht.requests:
			requests := make([]htRequest, 0, kRequestsBufferSize)
			requests = append(requests, req)
			for len(requests) < kRequestsBufferSize { // read all available requests
				select {
				case req := <-ht.requests:
					requests = append(requests, req)
				default:
					goto doRequests
				}
			}
		doRequests:
			responses := make([]htResponse, len(requests))
			anyPut := false
			for i := 0; i < len(requests); i++ {
				switch requests[i].reqType {
				case kPUT:
					responses[i].err = ht.doPut(requests[i].key, requests[i].offset)
					anyPut = true
				case kGET:
					responses[i].offset, responses[i].err = ht.doGet(requests[i].key)
				default:
					panic(fmt.Sprint("unexpected persistent hashtable request type:", requests[i].reqType))
				}
			}
			if anyPut && ht.currentFile != nil {
				if err := ht.currentFile.Sync(); err != nil {
					panic(err)
				}
			}
			for i := 0; i < len(requests); i++ {
				requests[i].callback(responses[i].offset, responses[i].err)
			}
		}
	}
}

func (ht *persistentHashTable) makeLogName(index uint64) string {
	return path.Join(ht.logDir, fmt.Sprintf("%06d.log", index))
}

// Restore -- reads the state from the disk
func (ht *persistentHashTable) Restore() error {
	for {
		logFileName := ht.makeLogName(ht.currentFileIndex)
		currentFile, err := os.OpenFile(logFileName, os.O_RDWR, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		var buffer KeyOffsetBuffer
		var record KeyOffset
		for {
			_, err = currentFile.Read(buffer[:])
			if err == io.EOF {
				break
			}
			record.deserialize(&buffer)
			ht.putNoLog(record.Key, record.Offset)
			ht.currentFileRecords += 1
		}
		if ht.currentFileRecords < kMaxRecordsPerLogFile {
			ht.currentFile = currentFile
			break
		}
		if err = currentFile.Close(); err != nil {
			return err
		}
		ht.currentFileIndex += 1
		ht.currentFileRecords = 0
	}
	return nil
}

func (ht *persistentHashTable) findSlot(key Key, data []htNode) uint64 {
	h := ht.hasher(key) % uint64(len(data))
	for data[h].isOccupied {
		if data[h].Key == key {
			return h
		}
		h += 1
		if h == uint64(len(data)) {
			h = 0
		}
	}
	return h
}

func (ht *persistentHashTable) extend() {
	newData := make([]htNode, int(float64(len(ht.data))*kScaleFactor))
	for _, node := range ht.data {
		if node.isOccupied {
			newData[ht.findSlot(node.Key, newData)] = node
		}
	}
	ht.data = newData
}

func (ht *persistentHashTable) putNoLog(key Key, offset Offset) {
	if float64(len(ht.data))*kMaxLoadFactor <= float64(ht.elements) {
		ht.extend()
	}
	slot := ht.findSlot(key, ht.data)
	if !ht.data[slot].isOccupied {
		ht.elements += 1
	}
	ht.data[slot] = htNode{KeyOffset{key, offset}, true}
}

func (ht *persistentHashTable) doLog(key Key, offset Offset) error {
	var err error
	if ht.currentFileRecords == kMaxRecordsPerLogFile {
		err = ht.currentFile.Sync()
		if err != nil {
			return err
		}
		err = ht.currentFile.Close()
		if err != nil {
			return err
		}
		ht.currentFile = nil
		ht.currentFileIndex += 1
		ht.currentFileRecords = 0
	}
	if ht.currentFile == nil {
		ht.currentFile, err = os.OpenFile(ht.makeLogName(ht.currentFileIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
	}
	var buf KeyOffsetBuffer
	keyOffset := KeyOffset{key, offset}
	keyOffset.serialize(&buf)
	_, err = ht.currentFile.Write(buf[:])
	if err != nil {
		return err
	}
	ht.currentFileRecords += 1
	return nil
}

func (ht *persistentHashTable) doPut(key Key, offset Offset) error {
	var err error
	err = ht.doLog(key, offset)
	if err != nil {
		return err
	}
	ht.putNoLog(key, offset)
	return nil
}

func (ht *persistentHashTable) doGet(key Key) (Offset, error) {
	slot := ht.findSlot(key, ht.data)
	if ht.data[slot].isOccupied && ht.data[slot].Key == key {
		return ht.data[slot].Offset, nil
	}
	return 0, KeyNotFoundError
}

func (ht *persistentHashTable) Size() uint64 {
	return ht.elements
}

func (ht *persistentHashTable) Put(key Key, offset Offset, callback func(error)) {
	ht.requests <- htRequest{
		reqType: kPUT,
		key:     key,
		offset:  offset,
		callback: func(_ Offset, err error) {
			callback(err)
		},
	}
}

func (ht *persistentHashTable) Get(key Key, callback func(Offset, error)) {
	ht.requests <- htRequest{
		reqType:  kGET,
		key:      key,
		callback: callback,
	}
}
