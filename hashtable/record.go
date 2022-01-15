package hashtable

import "github.com/vsalavatov/bsse-storage-systems/util"

const KeySize = 128

type Key = [KeySize]byte

type FileIndex = uint64
type Offset = uint64
type RecordLocation struct {
	FileIndex
	Offset
}

const RecordLocationSize = 8 + 8

type Version = uint32

const VersionSize = 4

type RecordMeta struct {
	RecordLocation
	Version
}

const RecordMetaSize = RecordLocationSize + VersionSize

type KeyMeta struct {
	Key
	RecordMeta
}

const KeyMetaSize = KeySize + RecordMetaSize

type KeyMetaBuffer = [KeyMetaSize]byte

func (kv *KeyMeta) serialize(buf *KeyMetaBuffer) {
	copy(buf[:KeySize], kv.Key[:])
	util.SerializeU64(kv.FileIndex, buf[KeySize:])
	util.SerializeU64(kv.Offset, buf[KeySize+8:])
	util.SerializeU32(kv.Version, buf[KeySize+8+8:])
}

func (kv *KeyMeta) deserialize(buf *KeyMetaBuffer) {
	copy(kv.Key[:], buf[:KeySize])
	kv.FileIndex = util.DeserializeU64(buf[KeySize:])
	kv.Offset = util.DeserializeU64(buf[KeySize+8:])
	kv.Version = util.DeserializeU32(buf[KeySize+8+8:])
}
