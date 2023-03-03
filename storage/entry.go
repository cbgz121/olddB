package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

var (
	//代表无效条目
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	//代表无效crc
	ErrInvalidCrc = errors.New("storage/entry: invalid crc")
)

const (
	// KeySize, ValueSize, ExtraSize, crc32 is uint32 type，4 bytes each.
	// Timestamp 8 bytes, tx 8 bytes, state 2 bytes.
	// 4 * 4 + 8 + 8 + 2 = 34
	//entry中的额外信息的大小
	entryHeaderSize = 34
)

//数据结构类型的值，目前只支持五种类型
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
	ALl
)

type (
	Entry struct {
		Meta      *Meta
		state     uint16 //state代表两个字段，高八位代表数据类型，低八位代表操作标志
		crc32     uint32 //crc检验数
		Timestamp uint64 //entry写入的时间
		TxId      uint64 //entry的事务id
	}

	//Meta 元信息
	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte //entry 的额外信息
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {
	return &Entry{
		state:     state,
		Timestamp: timestamp,
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
	}
}

//创建新的entry
func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	var state uint16 = 0
	//设置操作类型和操作标志位
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, value, extra, state, uint64(time.Now().UnixNano()))
}

//创建一个没有额外信息的新entry
func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry { //有什么用
	return NewEntry(key, value, nil, t, mark)
}

//使用过期信息创建一个新entry
func NewEntryWithExpire(key, value []byte, deadline int64, t, mark uint16) *Entry {
	var state uint16
	//设置操作类型的操作标志位
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, value, nil, state, uint64(deadline))
}

//使用事务信息创建一个新entry
func NewEntryWithTxn(key, value, extra []byte, t, mark uint16, txId uint64) *Entry {
	e := NewEntry(key, value, extra, t, mark)
	e.TxId = txId
	return e
}

//entry 的总大小
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

//将entry编码成二进制，返回一个字节切片
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.state)
	binary.BigEndian.PutUint64(buf[18:26], e.Timestamp)
	binary.BigEndian.PutUint64(buf[26:34], e.TxId)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Meta.Value)
	//因为可能没有额外信息，所以要单独判断一下
	if es > 0 {
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], e.Meta.Extra)
	}

	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

//解码entry并返回一个entry
func Decode(buf []byte) (*Entry, error) {
	crc := binary.BigEndian.Uint32(buf[0:4])
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	state := binary.BigEndian.Uint16(buf[16:18])
	timeStamp := binary.BigEndian.Uint64(buf[18:26])
	txId := binary.BigEndian.Uint64(buf[26:34])

	return &Entry{
		state:     state,
		Timestamp: timeStamp,
		TxId:      txId,
		crc32:     crc,
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
	}, nil
}

//获得高八位的数据类型
func (e *Entry) GetType() uint16 {
	return e.state >> 8
}

//获得低八位的操作类型
func (e *Entry) GetMark() uint16 {
	return e.state & (2<<7 - 1)
}
