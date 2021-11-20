package hashtable

import (
	"fmt"
	"io"
	"os"
	"path"
)

const kInitSize = 64
const kMaxLoadFactor = 0.75
const kScaleFactor = 1.66
const kMaxRecordsPerLogFile = 256 * 1024 * 1024 / KeyValueSize // about 256 MiB per log file

type htNode struct {
	KeyValue
	isOccupied bool
}

type PersistentHashTable interface {
	HashTable
	Restore() error
}

// persistentHashTable
// stores the sequence of put operations in log files in the logDir folder;
// not thread safe
type persistentHashTable struct {
	data     []htNode
	elements uint64
	hasher   Hasher

	logDir             string
	currentFile        *os.File
	currentFileIndex   uint64
	currentFileRecords uint64
}

func NewPersistentHashTable(hasher Hasher, logDir string) PersistentHashTable {
	ht := &persistentHashTable{
		data:               make([]htNode, kInitSize),
		elements:           0,
		hasher:             hasher,
		logDir:             logDir,
		currentFile:        nil,
		currentFileIndex:   0,
		currentFileRecords: 0,
	}
	return ht
}

func (ht *persistentHashTable) Free() error {
	if ht.currentFile != nil {
		err := ht.currentFile.Close()
		if err != nil {
			return err
		}
		ht.currentFile = nil
		ht.currentFileIndex = 0
		ht.currentFileRecords = 0
	}
	ht.elements = 0
	ht.data = make([]htNode, kInitSize)
	return nil
}

func (ht *persistentHashTable) makeLogName(index uint64) string {
	return path.Join(ht.logDir, fmt.Sprintf("%06d.log", index))
}

// Restore -- reads the state from the disk
func (ht *persistentHashTable) Restore() error {
	err := ht.Free()
	if err != nil {
		return err
	}
	for {
		logFileName := ht.makeLogName(ht.currentFileIndex)
		currentFile, err := os.OpenFile(logFileName, os.O_RDWR, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		var buffer KeyValueBuffer
		var record KeyValue
		for {
			_, err = currentFile.Read(buffer[:])
			if err == io.EOF {
				break
			}
			record.deserialize(&buffer)
			ht.putNoLog(record.key, record.value)
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
		if data[h].key == key {
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
			newData[ht.findSlot(node.key, newData)] = node
		}
	}
	ht.data = newData
}

func (ht *persistentHashTable) putNoLog(key Key, value Value) {
	if float64(len(ht.data))*kMaxLoadFactor <= float64(ht.elements) {
		ht.extend()
	}
	slot := ht.findSlot(key, ht.data)
	if !ht.data[slot].isOccupied {
		ht.elements += 1
	}
	ht.data[slot] = htNode{KeyValue{key, value}, true}
}

func (ht *persistentHashTable) doLog(key Key, value Value) error {
	var err error
	if ht.currentFileRecords == kMaxRecordsPerLogFile {
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
	var buf KeyValueBuffer
	keyValue := KeyValue{key, value}
	keyValue.serialize(&buf)
	_, err = ht.currentFile.Write(buf[:])
	if err != nil {
		return err
	}
	ht.currentFileRecords += 1
	return nil
}

func (ht *persistentHashTable) Put(key Key, value Value) error {
	var err error
	err = ht.doLog(key, value)
	if err != nil {
		return err
	}
	ht.putNoLog(key, value)
	return nil
}

func (ht *persistentHashTable) Get(key Key) (Value, error) {
	slot := ht.findSlot(key, ht.data)
	if ht.data[slot].isOccupied && ht.data[slot].key == key {
		return ht.data[slot].value, nil
	}
	return 0, KeyNotFoundError
}

func (ht *persistentHashTable) Size() uint64 {
	return ht.elements
}
