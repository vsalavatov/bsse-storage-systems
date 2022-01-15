package hashtable

import (
	"path"
	"sync"
	"sync/atomic"
)

type RotationPHT struct {
	phts        [2]*PersistentHashTable
	_writeIndex int32 // must be accessed atomically

	commonLogDir string

	compactionRunning int32 // must be accessed atomically
	compactionErr     error
}

func NewRotationHashTable(hasher Hasher, logDir string) *RotationPHT {
	ht := &RotationPHT{
		phts:         [2]*PersistentHashTable{},
		_writeIndex:  0,
		commonLogDir: logDir,
	}
	ht.phts[0] = NewPersistentHashTable(hasher, path.Join(logDir, "p0"))
	ht.phts[1] = NewPersistentHashTable(hasher, path.Join(logDir, "p1"))
	return ht
}

func (r *RotationPHT) Restore() error {
	var wg sync.WaitGroup
	wg.Add(2)
	var errs = [2]error{nil, nil}
	restoreF := func(ind int) {
		errs[ind] = r.phts[ind].Restore()
		wg.Done()
	}
	go restoreF(0)
	go restoreF(1)
	wg.Wait()
	if errs[0] != nil {
		return errs[0]
	}
	return errs[1]
}

func (r *RotationPHT) Hasher() Hasher {
	return r.phts[0].Hasher()
}

func (r *RotationPHT) determineMostRecentRecord(key Key, keyHash uint64) (RecordMeta, error) {
	res0, err0 := r.phts[0].Get(key, keyHash)
	if err0 != nil && err0 != KeyNotFoundError {
		return RecordMeta{}, err0
	}
	res1, err1 := r.phts[1].Get(key, keyHash)
	if err1 != nil && err1 != KeyNotFoundError {
		return RecordMeta{}, err1
	}
	if err0 == KeyNotFoundError && err1 == KeyNotFoundError {
		return RecordMeta{}, KeyNotFoundError
	}
	if res0.Version < res1.Version {
		return res1, nil
	}
	return res0, nil
}

func (r *RotationPHT) SetNewLocation(key Key, keyHash uint64, location RecordLocation) (RecordMeta, error) {
	record, err := r.determineMostRecentRecord(key, keyHash)
	if err != nil && err != KeyNotFoundError {
		return RecordMeta{}, err
	}
	if err == KeyNotFoundError {
		return r.phts[r.writeIndex()].SetNewLocation(key, keyHash, location)
	}
	meta := RecordMeta{
		RecordLocation: location,
		Version:        record.Version + 1,
	}
	err = r.phts[r.writeIndex()].SetMeta(key, keyHash, meta)
	return meta, err
}

// returns success flag, new meta, error
func (r *RotationPHT) SetNewLocationForVersion(key Key, keyHash uint64, location RecordLocation, version Version) (bool, RecordMeta, error) {
	record, err := r.determineMostRecentRecord(key, keyHash)
	if err != nil { // KeyNotFound is handled by this too
		return false, RecordMeta{}, err
	}
	if record.Version != version {
		return false, RecordMeta{}, nil
	}
	meta := RecordMeta{
		RecordLocation: location,
		Version:        version + 1,
	}
	err = r.phts[r.writeIndex()].SetMeta(key, keyHash, meta)
	return true, meta, err
}

func (r *RotationPHT) Get(key Key, keyHash uint64) (RecordMeta, error) {
	return r.determineMostRecentRecord(key, keyHash)
}

func (r *RotationPHT) Sync() error {
	return r.phts[r.writeIndex()].Sync()
}

// total used size
func (r *RotationPHT) Size() uint64 {
	return r.phts[0].Size() + r.phts[1].Size()
}

// returns error of the last compaction routine
func (r *RotationPHT) IsRotationAvailable() (bool, error) {
	return atomic.LoadInt32(&r.compactionRunning) == 0, r.compactionErr
}

func (r *RotationPHT) writeIndex() int32 {
	return atomic.LoadInt32(&r._writeIndex)
}

// this function must be a barrier between put/put ops
func (r *RotationPHT) Rotate() {
	toCompactIndex := r.writeIndex()
	atomic.StoreInt32(&r._writeIndex, toCompactIndex^1)
	atomic.StoreInt32(&r.compactionRunning, 1)
	go func() {
		r.compactionErr = r.phts[toCompactIndex].DumpAndCompact()
		atomic.StoreInt32(&r.compactionRunning, 0)
	}()
}
