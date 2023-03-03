package utils

import (
	"bytes"
	"encoding/binary"

	"github.com/vmihailenco/msgpack/v5" //一种高效的二进制序列化格式。它允许你在多种语言(如JSON)之间交换数据。但它更快更小
)

//将key转换成byte字节类型
func EncodeKey(key interface{}) (res []byte, err error) {
	switch key := key.(type) {
	case []byte:
		return key, nil
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		buf := new(bytes.Buffer)
		err = binary.Write(buf, binary.BigEndian, key)
		return buf.Bytes(), err
	case int:
		val := uint64(key)
		p := make([]byte, 8)
		p[0] = byte(val >> 56)
		p[1] = byte(val >> 48)
		p[2] = byte(val >> 40)
		p[3] = byte(val >> 32)
		p[4] = byte(val >> 24)
		p[5] = byte(val >> 16)
		p[6] = byte(val >> 8)
		p[7] = byte(val)
		return p, err
	case string:
		return []byte(key), nil
	default:
		res, err = msgpack.Marshal(key) //序列化
		return
	}
}

//将value转换成byte字节类型
func EncodeValue(value interface{}) (res []byte, err error) {
	switch value := value.(type) {
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	default:
		res, err = msgpack.Marshal(value)  
		return
	}
}

//将value解码为dest
func DecodeValue(value []byte, dest interface{}) (err error) { //暂时不知道有什么用，在str.go的时候会调用到
	switch dest := dest.(type) {
	case *[]byte:
		*dest = value
	case *string:
		*dest = string(value)
	default:
		err = msgpack.Unmarshal(value, dest)
		return
	}
	return
}
