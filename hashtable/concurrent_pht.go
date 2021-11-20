package hashtable

import (
	"fmt"
	"os"
	"path"
	"sync"
)

type scatterPHT struct {
	phts    []PersistentHashTable
	mutexes []sync.Mutex

	hasher       Hasher
	commonLogDir string

	scatterBits int
	scatterSize int
	scatterMask uint64
}

func NewScatterPHT(scatterBits int, hasher Hasher, logDir string) PersistentHashTable {
	instance := &scatterPHT{
		phts:         nil,
		mutexes:      nil,
		commonLogDir: logDir,
		hasher:       hasher,
		scatterBits:  scatterBits,
		scatterMask:  uint64((1 << scatterBits) - 1),
		scatterSize:  1 << scatterBits,
	}

	instance.phts = make([]PersistentHashTable, instance.scatterSize)
	instance.mutexes = make([]sync.Mutex, instance.scatterSize)

	subHasher := func(key Key) uint64 {
		value := instance.hasher(key)
		return value >> instance.scatterBits
	}
	scatterHex := (scatterBits + 3) / 4
	fmtStr := fmt.Sprint("%0", scatterHex, "x")
	for i := 0; i < instance.scatterSize; i++ {
		logPath := path.Join(instance.commonLogDir, fmt.Sprintf(fmtStr, i))
		os.MkdirAll(logPath, 0744)
		instance.phts[i] = NewPersistentHashTable(subHasher, logPath)
	}
	return instance
}

func (s *scatterPHT) Put(key Key, value Value) error {
	h := s.hasher(key) & s.scatterMask
	s.mutexes[h].Lock()
	defer s.mutexes[h].Unlock()
	return s.phts[h].Put(key, value)
}

func (s *scatterPHT) Get(key Key) (Value, error) {
	h := s.hasher(key) & s.scatterMask
	s.mutexes[h].Lock()
	defer s.mutexes[h].Unlock()
	return s.phts[h].Get(key)
}

func (s *scatterPHT) Size() uint64 { // estimated
	var sz uint64 = 0
	for i := 0; i < s.scatterSize; i++ {
		sz += s.phts[i].Size()
	}
	return sz
}

func (s *scatterPHT) Restore() error {
	for i := 0; i < s.scatterSize; i++ {
		s.mutexes[i].Lock()
		err := s.phts[i].Restore()
		s.mutexes[i].Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
