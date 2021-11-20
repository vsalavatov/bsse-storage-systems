package hashtable

import (
	"fmt"
)

const KeySize = 128
const KeyValueSize = KeySize + 8

type Key = [KeySize]byte
type Value = uint64

type Hasher = func(Key) uint64

type KeyValue struct {
	key   Key
	value Value
}
type KeyValueBuffer = [KeyValueSize]byte

func (kv *KeyValue) serialize(buf *KeyValueBuffer) {
	copy(buf[:KeySize], kv.key[:])
	for i := 0; i < 8; i++ {
		buf[KeySize+i] = byte((kv.value >> (i * 8)) & 0xff)
	}
}

func (kv *KeyValue) deserialize(buf *KeyValueBuffer) {
	copy(kv.key[:], buf[:KeySize])
	kv.value = 0
	for i := 0; i < 8; i++ {
		kv.value |= uint64(buf[KeySize + i]) << (i * 8)
	}
}


var (
	KeyNotFoundError = fmt.Errorf("key not found")
)

type HashTable interface {
	Put(key Key, value Value) error
	Get(key Key) (Value, error)

	Size() uint64
}
