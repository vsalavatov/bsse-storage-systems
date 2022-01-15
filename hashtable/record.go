package hashtable

import "github.com/vsalavatov/bsse-storage-systems/util"

const KeySize = 128

type Key = [KeySize]byte

type FileIndex = uint32
type Offset = uint64
type RecordLocation struct {
	FileIndex
	Offset
}

const RecordLocationSize = 4 + 8

type Version = uint32
type RecordMeta struct {
	RecordLocation
	Version
}

const RecordMetaSize = RecordLocationSize + 4

type KeyMeta struct {
	Key
	RecordMeta
}

const KeyMetaSize = KeySize + RecordMetaSize

type KeyMetaBuffer = [KeyMetaSize]byte

func (kv *KeyMeta) serialize(buf *KeyMetaBuffer) {
	copy(buf[:KeySize], kv.Key[:])
	util.SerializeU32(kv.FileIndex, buf[KeySize:])
	util.SerializeU64(kv.Offset, buf[KeySize+4:])
	util.SerializeU32(kv.Version, buf[KeySize+4+8:])
}

func (kv *KeyMeta) deserialize(buf *KeyMetaBuffer) {
	copy(kv.Key[:], buf[:KeySize])
	kv.FileIndex = util.DeserializeU32(buf[KeySize:])
	kv.Offset = util.DeserializeU64(buf[KeySize+4:])
	kv.Version = util.DeserializeU32(buf[KeySize+4+8:])
}
