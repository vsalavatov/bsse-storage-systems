package hashtable

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
)

type scatterPHT struct {
	phts []PersistentHashTable

	hasher       Hasher
	commonLogDir string

	scatterBits int
	scatterSize int
	scatterMask uint64
}

func NewScatterPHT(ctx context.Context, scatterBits int, hasher Hasher, logDir string) PersistentHashTable {
	instance := &scatterPHT{
		phts:         nil,
		commonLogDir: logDir,
		hasher:       hasher,
		scatterBits:  scatterBits,
		scatterMask:  uint64((1 << scatterBits) - 1),
		scatterSize:  1 << scatterBits,
	}

	instance.phts = make([]PersistentHashTable, instance.scatterSize)

	subHasher := func(key Key) uint64 {
		offset := instance.hasher(key)
		return offset >> instance.scatterBits
	}
	scatterHex := (scatterBits + 3) / 4
	fmtStr := fmt.Sprint("%0", scatterHex, "x")
	for i := 0; i < instance.scatterSize; i++ {
		logPath := path.Join(instance.commonLogDir, fmt.Sprintf(fmtStr, i))
		os.MkdirAll(logPath, 0744)
		instance.phts[i] = NewPersistentHashTable(ctx, subHasher, logPath)
	}
	return instance
}

func (s *scatterPHT) Put(key Key, offset Offset, callback func(error)) {
	h := s.hasher(key) & s.scatterMask
	s.phts[h].Put(key, offset, callback)
}

func (s *scatterPHT) Get(key Key, callback func(Offset, error)) {
	h := s.hasher(key) & s.scatterMask
	s.phts[h].Get(key, callback)
}

func (s *scatterPHT) Size() uint64 { // estimated
	var sz uint64 = 0
	for i := 0; i < s.scatterSize; i++ {
		sz += s.phts[i].Size()
	}
	return sz
}

func (s *scatterPHT) Restore() error {
	var wg sync.WaitGroup
	wg.Add(s.scatterSize)
	var errr error = nil
	restf := func(i int) {
		defer wg.Done()
		err := s.phts[i].Restore()
		if err != nil {
			errr = err
		}
	}
	for i := 0; i < s.scatterSize; i++ {
		go restf(i)
	}
	wg.Wait()
	return errr
}
