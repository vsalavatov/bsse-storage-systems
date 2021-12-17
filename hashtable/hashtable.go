package hashtable

import (
	"fmt"
)

const KeySize = 128
const KeyValueSize = KeySize + 8

type Key = [KeySize]byte
type Offset = uint64

type Hasher = func(Key) uint64

type KeyValue struct {
	Key
	Offset
}
type KeyValueBuffer = [KeyValueSize]byte

func (kv *KeyValue) serialize(buf *KeyValueBuffer) {
	copy(buf[:KeySize], kv.Key[:])
	for i := 0; i < 8; i++ {
		buf[KeySize+i] = byte((kv.Offset >> (i * 8)) & 0xff)
	}
}

func (kv *KeyValue) deserialize(buf *KeyValueBuffer) {
	copy(kv.Key[:], buf[:KeySize])
	kv.Offset = 0
	for i := 0; i < 8; i++ {
		kv.Offset |= uint64(buf[KeySize+i]) << (i * 8)
	}
}

var (
	KeyNotFoundError = fmt.Errorf("Key not found")
)

type HashTable interface {
	Put(key Key, offset Offset, callback func(error))
	Get(key Key, callback func(Offset, error))

	Size() uint64
}
