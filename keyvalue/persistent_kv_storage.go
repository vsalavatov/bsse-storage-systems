package keyvalue

import (
	"context"
	"fmt"
	"github.com/vsalavatov/bsse-storage-systems/hashtable"
	"os"
	"path"
	"sync"
)

const kRequestsBufferSize = 16
const kMaxDataFileSize = 2 * 1024 * 1024 * 1024 // 2 GiB, 31 bits
const kIndexOffsetBits = 40                     // first 40 bits -- offset in file, last 24 bits -- file index

const (
	kPUT = iota
	kGET
)

type kvRequest struct {
	reqType  uint8
	key      Key
	value    Value
	callback func(Value, error)
}

type kvResponse struct {
	value Value
	err   error
}

type persistentKeyValueStorage struct {
	offsetHT hashtable.HashTable

	logDir           string
	currentFile      *os.File
	currentFileIndex uint64

	requests chan kvRequest
	ctx      context.Context
}

func NewPersistentKeyValueStorage(ctx context.Context, logDir string, ht hashtable.HashTable) (KeyValueStorage, error) {
	ds := &persistentKeyValueStorage{
		offsetHT:         ht,
		logDir:           logDir,
		currentFile:      nil,
		currentFileIndex: 0,
		requests:         make(chan kvRequest, kRequestsBufferSize),
		ctx:              ctx,
	}
	if err := ds.restore(); err != nil {
		return nil, err
	}
	go ds.run()
	return ds, nil
}

// prepares currentFile and currentFileIndex
func (kv *persistentKeyValueStorage) restore() error {
	for {
		logFileName := kv.makeLogName(kv.currentFileIndex)
		currentFile, err := os.OpenFile(logFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		stat, err := currentFile.Stat()
		if err != nil {
			currentFile.Close()
			return err
		}
		if stat.Size()+MaxValueSize+8 < kMaxDataFileSize {
			kv.currentFile = currentFile
			break
		}
		if err = currentFile.Close(); err != nil {
			return err
		}
		kv.currentFileIndex += 1
	}
	return nil
}

func (kv *persistentKeyValueStorage) run() {
	for {
		select {
		case <-kv.ctx.Done():
			return
		case req := <-kv.requests:
			requests := make([]kvRequest, 0, kRequestsBufferSize)
			requests = append(requests, req)
			for len(requests) < kRequestsBufferSize { // read all available requests
				select {
				case req := <-kv.requests:
					requests = append(requests, req)
				default:
					goto doRequests
				}
			}
		doRequests:
			responses := make([]kvResponse, len(requests))
			anyPut := false
			for i := 0; i < len(requests); i++ {
				switch requests[i].reqType {
				case kPUT:
					responses[i].err = kv.doPut(requests[i].key, requests[i].value)
					anyPut = true
				case kGET:
					responses[i].value, responses[i].err = kv.doGet(requests[i].key)
				default:
					panic(fmt.Sprint("unexpected persistent key-value request type:", requests[i].reqType))
				}
			}
			if anyPut && kv.currentFile != nil {
				if err := kv.currentFile.Sync(); err != nil {
					panic(err)
				}
			}
			for i := 0; i < len(requests); i++ {
				requests[i].callback(responses[i].value, responses[i].err)
			}
		}
	}
}

func (kv *persistentKeyValueStorage) makeLogName(index uint64) string {
	return path.Join(kv.logDir, fmt.Sprintf("%06d.data", index))
}

func (kv *persistentKeyValueStorage) Put(key Key, value Value, callback func(error)) {
	kv.requests <- kvRequest{
		reqType: kPUT,
		key:     key,
		value:   value,
		callback: func(_ Value, err error) {
			callback(err)
		},
	}
}

func (kv *persistentKeyValueStorage) Get(key Key, callback func(Value, error)) {
	kv.requests <- kvRequest{
		reqType:  kGET,
		key:      key,
		callback: callback,
	}
}

func (kv *persistentKeyValueStorage) doPut(key Key, value Value) error {
	if len(value) > MaxValueSize {
		return fmt.Errorf("value is too big")
	}
	err := kv.prepareForWrite()
	if err != nil {
		return err
	}

	stats, err := kv.currentFile.Stat()
	if err != nil {
		return err
	}
	offset := uint64(stats.Size())
	index := kv.currentFileIndex

	var sizeBuf [8]byte
	for i := 0; i < 8; i++ {
		sizeBuf[i] = byte((uint64(len(value)) >> (i * 8)) & 0xff)
	}
	_, err = kv.currentFile.Write(sizeBuf[:])
	if err != nil {
		return err
	}
	_, err = kv.currentFile.Write(value)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)

	htOffset := (index << kIndexOffsetBits) | offset

	var htErr error
	kv.offsetHT.Put(key, htOffset, func(err error) {
		htErr = err
		wg.Done()
	})
	wg.Wait()

	return htErr
}

func (kv *persistentKeyValueStorage) doGet(key Key) (Value, error) {
	var wg sync.WaitGroup
	wg.Add(1)

	var htOffset uint64
	var htErr error
	kv.offsetHT.Get(key, func(offset uint64, err error) {
		htErr = err
		htOffset = offset
		wg.Done()
	})
	wg.Wait()
	if htErr != nil {
		return nil, htErr
	}

	var index, offset uint64 = 0, 0
	index = htOffset >> kIndexOffsetBits
	offset = htOffset & ((uint64(1) << kIndexOffsetBits) - 1)

	dataFile, err := os.OpenFile(kv.makeLogName(index), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	var sizeBuf [8]byte
	_, err = dataFile.ReadAt(sizeBuf[:], int64(offset))
	if err != nil {
		return nil, err
	}
	size := uint64(0)
	for i := 0; i < 8; i++ {
		size |= uint64(sizeBuf[i]) << (i * 8)
	}
	if size >= MaxValueSize {
		return nil, fmt.Errorf("value is too big")
	}
	value := make([]byte, size)
	_, err = dataFile.ReadAt(value, int64(offset)+8)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (kv *persistentKeyValueStorage) prepareForWrite() error {
	var err error = nil
	if kv.currentFile == nil {
		kv.currentFile, err = os.OpenFile(kv.makeLogName(kv.currentFileIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
	}
	stats, err := kv.currentFile.Stat()
	if err != nil {
		return err
	}
	if stats.Size()+MaxValueSize+8 >= kMaxDataFileSize {
		err = kv.currentFile.Sync()
		if err != nil {
			return err
		}
		err = kv.currentFile.Close()
		if err != nil {
			return err
		}
		kv.currentFile = nil
		kv.currentFileIndex += 1
		kv.currentFile, err = os.OpenFile(kv.makeLogName(kv.currentFileIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}
