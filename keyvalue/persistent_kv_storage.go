package keyvalue

import (
	"context"
	"errors"
	"fmt"
	"github.com/vsalavatov/bsse-storage-systems/hashtable"
	"github.com/vsalavatov/bsse-storage-systems/util"
	"math/rand"
	"os"
	"path"
	"time"
)

const kMaxDataFileSize = 256 * 1024 * 1024 // 256 MiB

type PersistentKeyValueStorage struct {
	hashtable *hashtable.RotationPHT

	logDir           string
	currentFile      *os.File
	currentFileIndex uint64
	shouldSync       bool

	rnd        *rand.Rand
	cleanerRnd *rand.Rand
}

func NewPersistentKeyValueStorage(logDir string, hasher hashtable.Hasher) *PersistentKeyValueStorage {
	ns := time.Now().UnixNano()
	kvs := &PersistentKeyValueStorage{
		hashtable:        hashtable.NewRotationHashTable(hasher, path.Join(logDir, "hashtables")),
		logDir:           path.Join(logDir, "values"),
		currentFile:      nil,
		currentFileIndex: 0,
		shouldSync:       false,
		rnd:              rand.New(rand.NewSource(ns)),
		cleanerRnd:       rand.New(rand.NewSource(ns + 239)),
	}
	util.EnsureDirExists(kvs.logDir)
	return kvs
}

func (kv *PersistentKeyValueStorage) Restore() error {
	return kv.hashtable.Restore()
}

func (kv *PersistentKeyValueStorage) makeLogName(index uint64) string {
	return path.Join(kv.logDir, fmt.Sprintf("%016x.data", index))
}

func (kv *PersistentKeyValueStorage) findFreeLogIndex() uint64 {
	for {
		index := kv.rnd.Uint64()
		logName := kv.makeLogName(index)
		if _, err := os.Stat(logName); errors.Is(err, os.ErrNotExist) {
			return index
		}
	}
}

func (kv *PersistentKeyValueStorage) Put(key Key, value Value) error {
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
	location := RecordLocation{
		FileIndex: index,
		Offset:    offset,
	}

	meta, err := kv.hashtable.SetNewLocation(key, kv.hashtable.Hasher()(key), location)
	if err != nil {
		return err
	}
	header := RecordHeader{
		Key:     key,
		Length:  uint64(len(value)),
		Version: meta.Version,
	}

	var headerBuf RecordHeaderBuffer
	header.serialize(&headerBuf)

	_, err = kv.currentFile.Write(headerBuf[:])
	if err != nil {
		return err
	}
	_, err = kv.currentFile.Write(value)
	if err != nil {
		return err
	}
	kv.shouldSync = true

	return nil
}

func (kv *PersistentKeyValueStorage) Get(key Key) (Value, error) {
	meta, err := kv.hashtable.Get(key, kv.hashtable.Hasher()(key))
	if err != nil {
		return nil, err
	}

	dataFile, err := os.OpenFile(kv.makeLogName(meta.FileIndex), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	var headerBuf RecordHeaderBuffer
	_, err = dataFile.ReadAt(headerBuf[:], int64(meta.Offset))
	if err != nil {
		return nil, err
	}

	var header RecordHeader
	header.deserialize(&headerBuf)

	value := make([]byte, header.Length)
	_, err = dataFile.ReadAt(value, int64(meta.Offset)+RecordHeaderSize)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (kv *PersistentKeyValueStorage) prepareForWrite() error {
	var err error = nil
	if kv.currentFile == nil {
		kv.currentFileIndex = kv.findFreeLogIndex()
		kv.currentFile, err = os.OpenFile(kv.makeLogName(kv.currentFileIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
	}
	stats, err := kv.currentFile.Stat()
	if err != nil {
		return err
	}
	if stats.Size()+MaxValueSize+RecordHeaderSize >= kMaxDataFileSize {
		err = kv.currentFile.Sync()
		if err != nil {
			return err
		}
		kv.shouldSync = false
		err = kv.currentFile.Close()
		if err != nil {
			return err
		}
		kv.currentFile = nil
		kv.currentFileIndex = kv.findFreeLogIndex()
		kv.currentFile, err = os.OpenFile(kv.makeLogName(kv.currentFileIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func (kv *PersistentKeyValueStorage) Sync() error {
	if kv.shouldSync && kv.currentFile != nil {
		if err := kv.currentFile.Sync(); err != nil {
			return err
		}
		kv.shouldSync = false
	}
	return kv.hashtable.Sync()
}

func (kv *PersistentKeyValueStorage) IsRotationAvailable() (bool, error) {
	return kv.hashtable.IsRotationAvailable()
}

func (kv *PersistentKeyValueStorage) Rotate() {
	kv.hashtable.Rotate()
}

func (kv *PersistentKeyValueStorage) CleanerJob(ctx context.Context) {
	// TODO
}
