package hashtable

import (
	"fmt"
)

const kInitSize = 64
const kMaxLoadFactor = 0.75
const kScaleFactor = 1.66

type Hasher = func(Key) uint64

var (
	KeyNotFoundError = fmt.Errorf("key not found")
)

type HashtableNode struct {
	KeyMeta
	isOccupied bool
}

type HashTable struct {
	data     []HashtableNode
	elements uint64
	Hasher   Hasher
}

func NewHashTable(hasher Hasher) *HashTable {
	return &HashTable{
		data:     make([]HashtableNode, kInitSize),
		elements: 0,
		Hasher:   hasher,
	}
}

func (ht *HashTable) findSlot(key Key, keyHash uint64, data []HashtableNode) uint64 {
	for data[keyHash].isOccupied {
		if data[keyHash].Key == key {
			return keyHash
		}
		keyHash += 1
		if keyHash == uint64(len(data)) {
			keyHash = 0
		}
	}
	return keyHash
}

func (ht *HashTable) extend() {
	newData := make([]HashtableNode, int(float64(len(ht.data))*kScaleFactor))
	for _, node := range ht.data {
		if node.isOccupied {
			newData[ht.findSlot(node.Key, ht.Hasher(node.Key), newData)] = node
		}
	}
	ht.data = newData
}

func (ht *HashTable) SetMeta(key Key, keyHash uint64, meta RecordMeta) {
	ht.ensureCapacity()
	slot := ht.findSlot(key, keyHash, ht.data)
	if !ht.data[slot].isOccupied {
		ht.elements += 1
	}
	ht.data[slot] = HashtableNode{KeyMeta{key, meta}, true}
}

func (ht *HashTable) SetNewLocation(key Key, keyHash uint64, location RecordLocation) RecordMeta {
	ht.ensureCapacity()
	slot := ht.findSlot(key, keyHash, ht.data)
	var meta RecordMeta
	if !ht.data[slot].isOccupied {
		ht.elements += 1
		meta = RecordMeta{
			RecordLocation: location,
			Version:        1,
		}
	} else {
		meta = RecordMeta{
			RecordLocation: location,
			Version:        ht.data[slot].Version + 1,
		}
	}
	ht.data[slot] = HashtableNode{KeyMeta{key, meta}, true}
	return meta
}

// returns success flag, new record meta, error
func (ht *HashTable) SetNewLocationForVersion(key Key, keyHash uint64, location RecordLocation, version Version) (bool, RecordMeta, error) {
	slot := ht.findSlot(key, keyHash, ht.data)
	if !ht.data[slot].isOccupied {
		return false, RecordMeta{}, KeyNotFoundError
	}
	if ht.data[slot].Version == version {
		var meta = RecordMeta{
			RecordLocation: location,
			Version:        version + 1,
		}
		ht.data[slot] = HashtableNode{KeyMeta{key, meta}, true}
		return true, meta, nil
	}
	return false, ht.data[slot].RecordMeta, nil
}

func (ht *HashTable) ensureCapacity() {
	if float64(len(ht.data))*kMaxLoadFactor <= float64(ht.elements) {
		ht.extend()
	}
}

func (ht *HashTable) Get(key Key, keyHash uint64) (RecordMeta, error) {
	slot := ht.findSlot(key, keyHash, ht.data)
	if ht.data[slot].isOccupied && ht.data[slot].Key == key {
		return ht.data[slot].RecordMeta, nil
	}
	return RecordMeta{}, KeyNotFoundError
}

func (ht *HashTable) Size() uint64 {
	return ht.elements
}
