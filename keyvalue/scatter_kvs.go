package keyvalue

//
//import (
//	"context"
//	"fmt"
//	"github.com/vsalavatov/bsse-storage-systems/hashtable"
//	"os"
//	"path"
//)
//
//type scatterKVS struct {
//	pht    hashtable.HashTable
//	subkvs []KeyValueStorage
//
//	hasher        hashtable.Hasher
//	commonDataDir string
//
//	instancesBits int
//	instances     int
//	instancesMask int
//}
//
//func NewScatterKVS(
//	ctx context.Context,
//	instancesBits int,
//	hasher hashtable.Hasher,
//	dataDir string,
//	pht hashtable.HashTable,
//) (KeyValueStorage, error) {
//	kvs := &scatterKVS{
//		pht:           pht,
//		subkvs:        make([]KeyValueStorage, 1<<instancesBits),
//		hasher:        hasher,
//		commonDataDir: dataDir,
//		instancesBits: instancesBits,
//		instances:     1 << instancesBits,
//		instancesMask: (1 << instancesBits) - 1,
//	}
//	scatterHex := (instancesBits + 3) / 4
//	fmtStr := fmt.Sprint("%0", scatterHex, "x")
//	for i := 0; i < kvs.instances; i++ {
//		logPath := path.Join(kvs.commonDataDir, fmt.Sprintf(fmtStr, i))
//		os.MkdirAll(logPath, 0744)
//		var err error
//		kvs.subkvs[i], err = NewPersistentKeyValueStorage(ctx, logPath, pht)
//		if err != nil {
//			return nil, err
//		}
//	}
//	return kvs, nil
//}
//
//func (s *scatterKVS) Put(key Key, value Value, callback func(error)) {
//	h := s.hasher(key) & uint64(s.instancesMask)
//	s.subkvs[h].Put(key, value, callback)
//}
//
//func (s *scatterKVS) Get(key Key, callback func(Value, error)) {
//	h := s.hasher(key) & uint64(s.instancesMask)
//	s.subkvs[h].Get(key, callback)
//}
