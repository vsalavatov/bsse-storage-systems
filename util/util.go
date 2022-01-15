package util

func SerializeU64(value uint64, buf []byte) {
	for i := 0; i < 8; i++ {
		buf[i] = byte((value >> (i * 8)) & 0xff)
	}
}

func DeserializeU64(buf []byte) uint64 {
	val := uint64(0)
	for i := 0; i < 8; i++ {
		val |= uint64(buf[i]) << (i * 8)
	}
	return val
}

func SerializeU32(value uint32, buf []byte) {
	for i := 0; i < 4; i++ {
		buf[i] = byte((value >> (i * 8)) & 0xff)
	}
}

func DeserializeU32(buf []byte) uint32 {
	val := uint32(0)
	for i := 0; i < 4; i++ {
		val |= uint32(buf[i]) << (i * 8)
	}
	return val
}
