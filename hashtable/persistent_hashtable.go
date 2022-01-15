package hashtable

import (
	"fmt"
	"github.com/vsalavatov/bsse-storage-systems/util"
	"io"
	"os"
	"path"
)

const kMaxRecordsPerLogFile = 16 * 1024 * 1024 / KeyMetaSize // about 16 MiB per log file

// PersistentHashTable
// stores the sequence of put operations in log files in the logDir folder;
type PersistentHashTable struct {
	hashtable *HashTable

	logDir             string
	currentFile        *os.File
	currentFileIndex   uint32
	currentFileRecords uint64
	shouldSync         bool
}

func NewPersistentHashTable(hasher Hasher, logDir string) *PersistentHashTable {
	util.EnsureDirExists(logDir)
	ht := &PersistentHashTable{
		hashtable:          NewHashTable(hasher),
		logDir:             logDir,
		currentFile:        nil,
		currentFileIndex:   0,
		currentFileRecords: 0,
		shouldSync:         false,
	}
	return ht
}

func (pht *PersistentHashTable) makeLogName(index uint32) string {
	return path.Join(pht.logDir, fmt.Sprintf("%06d.log", index))
}

func (pht *PersistentHashTable) makeDataName() string {
	return path.Join(pht.logDir, "data.bin")
}

func (pht *PersistentHashTable) Hasher() Hasher {
	return pht.hashtable.Hasher
}

// Restore -- reads the state from the disk
func (pht *PersistentHashTable) Restore() error {
	{ // read
		dataFile, err := os.OpenFile(pht.makeDataName(), os.O_RDONLY, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				goto nodata
			}
			return err
		}
		defer dataFile.Close()
		var buffer KeyMetaBuffer
		var record KeyMeta
		for {
			_, err = dataFile.Read(buffer[:])
			if err == io.EOF {
				break
			}
			record.deserialize(&buffer)
			pht.hashtable.SetMeta(record.Key, pht.hashtable.Hasher(record.Key), record.RecordMeta)
		}
	}
nodata:
	for {
		logFileName := pht.makeLogName(pht.currentFileIndex)
		currentFile, err := os.OpenFile(logFileName, os.O_RDWR, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		var buffer KeyMetaBuffer
		var record KeyMeta
		for {
			_, err = currentFile.Read(buffer[:])
			if err == io.EOF {
				break
			}
			record.deserialize(&buffer)
			pht.hashtable.SetMeta(record.Key, pht.hashtable.Hasher(record.Key), record.RecordMeta)
			pht.currentFileRecords += 1
		}
		if pht.currentFileRecords < kMaxRecordsPerLogFile {
			pht.currentFile = currentFile
			break
		}
		if err = currentFile.Close(); err != nil {
			return err
		}
		pht.currentFileIndex += 1
		pht.currentFileRecords = 0
	}
	return nil
}

// there must be no concurrent put operations until this method completes
func (pht *PersistentHashTable) DumpAndCompact() error {
	dataFile, err := os.OpenFile(pht.makeDataName()+".tmp", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	for _, elem := range pht.hashtable.data {
		if elem.isOccupied {
			var buf KeyMetaBuffer
			elem.KeyMeta.serialize(&buf)
			_, err = dataFile.Write(buf[:])
		}
	}
	err = dataFile.Sync()
	if err != nil {
		return err
	}
	err = dataFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(pht.makeDataName()+".tmp", pht.makeDataName())
	if err != nil { // replace old data.bin with the new one
		return err
	}
	// now we can delete all log files
	if pht.currentFile != nil {
		err = pht.currentFile.Sync()
		if err != nil {
			return err
		}
		err = pht.currentFile.Close()
		if err != nil {
			return err
		}
		pht.currentFile = nil
	}
	for i := uint32(0); i <= pht.currentFileIndex; i++ {
		err = os.Remove(pht.makeLogName(i))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	pht.currentFileIndex = 0
	pht.currentFileRecords = 0
	pht.shouldSync = false
	return nil
}

func (pht *PersistentHashTable) writeToLog(key Key, meta RecordMeta) error {
	err := pht.prepareForLogWrite()
	if err != nil {
		return err
	}
	var buf KeyMetaBuffer
	keyMeta := KeyMeta{key, meta}
	keyMeta.serialize(&buf)
	_, err = pht.currentFile.Write(buf[:])
	if err != nil {
		return err
	}
	pht.currentFileRecords += 1
	pht.shouldSync = true
	return nil
}

func (pht *PersistentHashTable) prepareForLogWrite() error {
	var err error = nil
	if pht.currentFileRecords == kMaxRecordsPerLogFile {
		err = pht.currentFile.Sync()
		if err != nil {
			return err
		}
		pht.shouldSync = false
		err = pht.currentFile.Close()
		if err != nil {
			return err
		}
		pht.currentFile = nil
		pht.currentFileIndex += 1
		pht.currentFileRecords = 0
	}
	if pht.currentFile == nil {
		pht.currentFile, err = os.OpenFile(pht.makeLogName(pht.currentFileIndex), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pht *PersistentHashTable) SetMeta(key Key, keyHash uint64, meta RecordMeta) error {
	pht.hashtable.SetMeta(key, keyHash, meta)
	return pht.writeToLog(key, meta)
}

func (pht *PersistentHashTable) SetNewLocation(key Key, keyHash uint64, location RecordLocation) (RecordMeta, error) {
	meta := pht.hashtable.SetNewLocation(key, keyHash, location)
	if err := pht.writeToLog(key, meta); err != nil {
		return RecordMeta{}, err
	}
	return RecordMeta{}, nil
}

// returns success flag
func (pht *PersistentHashTable) SetNewLocationForVersion(key Key, keyHash uint64, location RecordLocation, version Version) (bool, RecordMeta, error) {
	success, meta, err := pht.hashtable.SetNewLocationForVersion(key, keyHash, location, version)
	if err != nil {
		return false, RecordMeta{}, err
	}
	if success {
		if err := pht.writeToLog(key, meta); err != nil {
			return false, RecordMeta{}, err
		}
	}
	return success, meta, nil
}

func (pht *PersistentHashTable) Get(key Key, keyHash uint64) (RecordMeta, error) {
	return pht.hashtable.Get(key, keyHash)
}

func (pht *PersistentHashTable) Sync() error {
	if pht.shouldSync && pht.currentFile != nil {
		err := pht.currentFile.Sync()
		if err != nil {
			return err
		}
		pht.shouldSync = false
	}
	return nil
}

func (pht *PersistentHashTable) Size() uint64 {
	return pht.hashtable.Size()
}
