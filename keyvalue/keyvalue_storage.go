package keyvalue

import "github.com/vsalavatov/bsse-storage-systems/hashtable"

const MaxValueSize = 4 * 1024 * 1024 // 4 MiB

type Key = hashtable.Key
type Value = []byte

type KeyValue struct {
	key   Key
	value Value
}

type KeyValueStorage interface {
	Put(key Key, value Value, callback func(error))
	Get(key Key, callback func(Value, error))
}
