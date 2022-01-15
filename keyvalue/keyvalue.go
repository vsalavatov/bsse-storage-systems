package keyvalue

import (
	"github.com/vsalavatov/bsse-storage-systems/hashtable"
	"github.com/vsalavatov/bsse-storage-systems/util"
)

const MaxValueSize = 4 * 1024 * 1024 // 4 MiB

type RecordLocation = hashtable.RecordLocation
type Key = hashtable.Key

const KeySize = hashtable.KeySize

type Version = hashtable.Version

const VersionSize = hashtable.VersionSize

type Value = []byte
type Length = uint64

type RecordHeader struct {
	Key
	Length
	Version
}

const RecordHeaderSize = KeySize + VersionSize + 8

type RecordHeaderBuffer = [RecordHeaderSize]byte

type Record struct {
	RecordHeader
	Value
}

func (h *RecordHeader) serialize(buf *RecordHeaderBuffer) {
	copy(buf[:KeySize], h.Key[:])
	util.SerializeU64(h.Length, buf[KeySize:])
	util.SerializeU32(h.Version, buf[KeySize+8:])
}

func (h *RecordHeader) deserialize(buf *RecordHeaderBuffer) {
	copy(h.Key[:], buf[:KeySize])
	h.Length = util.DeserializeU64(buf[KeySize:])
	h.Version = util.DeserializeU32(buf[KeySize+8:])
}
