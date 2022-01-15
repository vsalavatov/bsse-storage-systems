package keyvalue

import (
	"context"
	"errors"
	"fmt"
	"github.com/vsalavatov/bsse-storage-systems/hashtable"
	"github.com/vsalavatov/bsse-storage-systems/util"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

const kMaxDataFileSize = 256 * 1024 * 1024 // 256 MiB
const kCleanerWakeupPeriodSeconds = 5
const kCleanerDeleteGracePeriodSeconds = 10

type PersistentKeyValueStorage struct {
	hashtable *hashtable.RotationPHT
	htMutex   sync.Mutex

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
		htMutex:          sync.Mutex{},
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

func (kv *PersistentKeyValueStorage) parseLogIndex(filename string) (uint64, error) {
	var index uint64
	n, _ := fmt.Sscanf(filename, "%016x.data", &index)
	if n != 1 {
		return 0, fmt.Errorf(fmt.Sprint("failed to parse index of ", filename))
	}
	return index, nil
}

func (kv *PersistentKeyValueStorage) findFreeLogIndex(rnd *rand.Rand) uint64 {
	for {
		index := rnd.Uint64()
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
	kv.htMutex.Lock()
	meta, err := kv.hashtable.SetNewLocation(key, kv.hashtable.Hasher()(key), location)
	kv.htMutex.Unlock()
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
	kv.htMutex.Lock()
	meta, err := kv.hashtable.Get(key, kv.hashtable.Hasher()(key))
	kv.htMutex.Unlock()
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
		kv.currentFileIndex = kv.findFreeLogIndex(kv.rnd)
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
		kv.currentFileIndex = kv.findFreeLogIndex(kv.rnd)
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
	kv.htMutex.Lock()
	defer kv.htMutex.Unlock()
	return kv.hashtable.Sync()
}

func (kv *PersistentKeyValueStorage) IsRotationAvailable() (bool, error) {
	kv.htMutex.Lock()
	defer kv.htMutex.Unlock()
	return kv.hashtable.IsRotationAvailable()
}

func (kv *PersistentKeyValueStorage) Rotate() {
	kv.htMutex.Lock()
	kv.hashtable.Rotate()
	kv.htMutex.Unlock()
}

func (kv *PersistentKeyValueStorage) CleanerJob(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			break
		case <-time.NewTimer(kCleanerWakeupPeriodSeconds * time.Second).C:
			kv.cleanOnce(ctx)
		}
	}
}

func (kv *PersistentKeyValueStorage) cleanerReportError(msg string, err error) {
	fmt.Println("[cleaner] error:", msg, err.Error())
}

func cleanerDelay() <-chan time.Time {
	return time.NewTimer(8 * time.Millisecond).C
}

func (kv *PersistentKeyValueStorage) countActualAndObsoleteRecordsInLog(logName string, logIndex uint64) (actual int, obsolete int, err error) {
	logfile, err := os.OpenFile(path.Join(kv.logDir, logName), os.O_RDONLY, 0644)
	if err != nil {
		kv.cleanerReportError("failed to open log", err)
		return 0, 0, err
	}
	defer logfile.Close()

	offset := int64(0)
	for { // read logfile
		var headerBuf RecordHeaderBuffer
		_, err = logfile.ReadAt(headerBuf[:], offset)
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			kv.cleanerReportError("unexpected error while reading log", err)
			return 0, 0, err
		}
		var header RecordHeader
		header.deserialize(&headerBuf)

		kv.htMutex.Lock()
		meta, err := kv.hashtable.Get(header.Key, kv.hashtable.Hasher()(header.Key))
		kv.htMutex.Unlock()
		if err != nil {
			kv.cleanerReportError("hashtable.get returned an error", err)
			return 0, 0, err
		}

		if meta.FileIndex != logIndex || meta.Version != header.Version {
			obsolete += 1
		} else {
			actual += 1
		}
		offset += int64(header.Length) + RecordHeaderSize
	}
	return
}

func (kv *PersistentKeyValueStorage) cleanOnce(ctx context.Context) {
	files, err := listDir(kv.logDir)
	if err != nil {
		kv.cleanerReportError("failed to list dir", err)
		return
	}
	if len(files) == 0 {
		return
	}
	kv.cleanerRnd.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
	logName := files[0]
	logIndex, err := kv.parseLogIndex(logName)
	if err != nil {
		kv.cleanerReportError("", err)
		return
	}
	if logIndex == kv.currentFileIndex {
		return
	}

	cntActual, cntObsolete, err := kv.countActualAndObsoleteRecordsInLog(logName, logIndex)
	if err != nil {
		kv.cleanerReportError("failed to calculate log statistics", err)
		return
	}
	totalRecords := cntActual + cntObsolete
	if totalRecords < 1 {
		totalRecords = 1
	}
	if float64(cntObsolete)/float64(totalRecords) < 0.1 {
		return
	}

	newIndex := kv.findFreeLogIndex(kv.cleanerRnd)
	newLogFile, err := os.OpenFile(kv.makeLogName(newIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		kv.cleanerReportError("failed to create new log file", err)
		return
	}
	defer newLogFile.Close()
	newLogWritten := uint64(0)

	isNewLogFilled := func() bool {
		return newLogWritten+MaxValueSize+RecordHeaderSize >= kMaxDataFileSize
	}

	type UpdateInfo struct {
		key         Key
		version     Version
		newLocation hashtable.RecordLocation
	}
	var updates = make([]UpdateInfo, 0, 16)
	var deleteList = make([]string, 0)

	for _, logName := range files {
		if isNewLogFilled() {
			break
		}
		logIndex, err := kv.parseLogIndex(logName)
		if err != nil {
			kv.cleanerReportError("", err)
			return
		}
		if logIndex == kv.currentFileIndex {
			return
		}
		logfile, err := os.OpenFile(path.Join(kv.logDir, logName), os.O_RDONLY, 0644)
		if err != nil {
			kv.cleanerReportError("failed to open log", err)
			return
		}

		offset := int64(0)
		mayDeleteLog := true
		for {
			if isNewLogFilled() {
				mayDeleteLog = false
				break
			}
			var headerBuf RecordHeaderBuffer
			_, err = logfile.ReadAt(headerBuf[:], offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				kv.cleanerReportError("unexpected error while reading log", err)
				logfile.Close()
				return
			}
			var header RecordHeader
			header.deserialize(&headerBuf)

			kv.htMutex.Lock()
			meta, err := kv.hashtable.Get(header.Key, kv.hashtable.Hasher()(header.Key))
			kv.htMutex.Unlock()
			if err != nil {
				kv.cleanerReportError("hashtable.get returned an error", err)
				logfile.Close()
				return
			}

			if meta.FileIndex == logIndex && meta.Version == header.Version {
				value := make([]byte, header.Length)
				_, err = logfile.ReadAt(value, int64(offset)+RecordHeaderSize)
				if err != nil {
					kv.cleanerReportError("failed to read a value from log", err)
					logfile.Close()
					return
				}
				_, err = newLogFile.Write(headerBuf[:])
				if err != nil {
					kv.cleanerReportError("failed to write header to new log", err)
					logfile.Close()
					return
				}
				_, err = newLogFile.Write(value[:])
				if err != nil {
					kv.cleanerReportError("failed to write value to new log", err)
					logfile.Close()
					return
				}
				updates = append(updates, UpdateInfo{
					header.Key,
					meta.Version,
					RecordLocation{
						FileIndex: newIndex,
						Offset:    uint64(newLogWritten),
					},
				})
				newLogWritten += RecordHeaderSize + uint64(header.Length)
			}
			offset += int64(header.Length) + RecordHeaderSize
		}
		logfile.Close()

		if mayDeleteLog {
			deleteList = append(deleteList, logName)
		}
	}

	err = newLogFile.Sync()
	if err != nil {
		kv.cleanerReportError("failed to sync new log", err)
		return
	}
	newLogFile.Close()

	ok := true
	kv.htMutex.Lock()
	for _, upd := range updates {
		_, _, err := kv.hashtable.SetNewLocationForVersion(upd.key, kv.hashtable.Hasher()(upd.key), upd.newLocation, upd.version)
		if err != nil {
			ok = false
			kv.cleanerReportError("failed to update verison location in hashtable", err)
			break
		}
	}
	kv.htMutex.Unlock()
	if ok && len(deleteList) > 0 {
		// schedule log deletion
		time.Sleep(kCleanerDeleteGracePeriodSeconds * time.Second)
		for _, target := range deleteList {
			err := os.Remove(path.Join(kv.logDir, target))
			if err != nil {
				kv.cleanerReportError("failed to delete log", err)
			} else {
				fmt.Println("[cleaner] purged", target, "log")
			}
		}
	}
}
