package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/edsrzf/mmap-go"
)

const (
	//新建db文件的默认权限
	FilePerm = 0644

	//默认路径分隔符
	PathSeparator = string(os.PathSeparator)

	//临时目录，仅存在于合并的时候
	mergeDir = "rosedb_merge"
)

var (
	//db数据文件格式
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	//表示db文件的后缀名
	DBFileSuffixName = map[string]uint16{"str": 0, "list": 1, "hash": 2, "set": 3, "zset": 4}
)

var (
	//自定义错误：entry是空的
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
	//自定义错误：entry超长
	ErrEntryTooLarge = errors.New("storage/db_file: entry is too large to store in mmap mode")
)

//文件读写方法
type FileRWMethod uint8

//ArchivedFiles定义归档文件，这意味着这些文件只能被读取，并且永远不会开放写
type ArchivedFiles map[uint16]map[uint32]*DBFile
type FileIds map[uint16]uint32

const (
	//FileIO 表示使用系统标准IO读写数据文件
	FileIO FileRWMethod = iota

	//MMap 表示使用mmap读写数据文件
	MMap
)

type DBFile struct {
	Id     uint32
	Path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod //不同文件有不同的读写方法，根据相应的读写方法去读写文件
}

//open的时候，用O_CREAT一个新文件后，mmap创建映射区可以成功么？
//答：不能mmap成功，因为O_CREAT新创建文件后，大小为0---->要想mmap成功，需要先对文件进行大小扩展（lseek/truncate）
//blockSize相当于是预先指定文件的大小，因为文件刚创建时大小为0，不能进行mmap内存映射，所以用blockSize调用truncate截断文件，
//相当于是给文件一个大小然后才能进行mmap内存映射
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	//格式化数据文件名
	//例如：path = /tmp/simpledb eType = 0 fileId = 2 则 filePath = /tmp/simpledb/000000002.data.str
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, Path: path, Offset: stat.Size(), method: method}

	df.File = file
	if method == MMap {
		if err = file.Truncate(blockSize); err != nil { //从blockSize处开始截断文件，只保留前blockSize大小的文件，后面的全部删除
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0) //这里会有mmap的原理问题,mmap方法有助于持久化数据
		if err != nil {
			return nil, err
		}
		df.mmap = m //如果操作文件方法是MMap那以后就根据df.mmap修改文件，df.mmap就是文件的映射
	}
	return df, nil
}

//从offset处开始读取文件内容
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte

	//读取entry头信息
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	// read key if necessary(by the KeySize)
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	// read value if necessary.
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var value []byte
		if value, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = value
	}

	//read extra info if necessary
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var extra []byte
		if extra, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = extra
	}

	checkCc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCc != e.crc32 {
		return nil, ErrInvalidCrc
	}
	return
}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap {
		if offset > int64(len(df.mmap)) { // 因为类型是mmap的话文件大小有限制，所以要先用offset比较大小
			return nil, io.EOF
		}
		copy(buf, df.mmap[offset:]) //只从mmap文件中读取len(buf)大小的数据放入buf中
	}
	return buf, nil
}

//将数据从文件offset处开始写入db文件中
func (df *DBFile) Write(e *Entry) (err error) {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method, offset := df.method, df.Offset
	var encVal []byte
	if encVal, err = e.Encode(); err != nil {
		return
	}

	if method == FileIO {
		if _, err = df.File.WriteAt(encVal, offset); err != nil {
			return
		}
	}

	if method == MMap {
		if offset+int64(len(encVal)) > int64(len(df.mmap)) {
			return ErrEntryTooLarge
		}
		copy(df.mmap[offset:], encVal)
	}
	df.Offset += int64(e.Size())
	return
}

//关闭文件, sync表示关闭前是否持久化数据
func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}

	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}

//将文件持久化到磁盘中
//当用户程序对文件系统进行修改以后，例如进行了写操作，文件数据把修改记录在内核缓冲中，在数据没有写到磁盘的时候，依然能够执行用户进程。
//磁盘的数据更新实际上是异步进行的，很有可能在写操作已经完成很长时间以后才真正对磁盘的数据进行更新。sync命令 强制把磁盘缓冲的所有数据写入磁盘
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

//更新文件写入位置的偏移量
func (df *DBFile) SetOffset(offset int64) {
	df.Offset = offset
}

//寻找有效的entry
func (df *DBFile) FindValidEntries(validFn func(*Entry, int64, uint32) bool) (entries []*Entry, err error) {
	var offset int64 = 0
	for {
		var e *Entry
		if e, err = df.Read(offset); err == nil {
			if validFn(e, offset, df.Id) { //validFn判断entry是否过期
				entries = append(entries, e)
			}
			offset += int64(e.Size())
		} else {
			if err == io.EOF {
				break
			}
			err = errors.New(fmt.Sprintf("read entry err.[%+v]", err))
			return
		}
	}
	return
}

//单独加载某个类型的文件
func BuildType(path string, method FileRWMethod, blockSize int64, eType uint16) (ArchivedFiles, FileIds, error) {
	return buildInternal(path, method, blockSize, eType)
}

//从磁盘加载所有的文件
func Build(path string, method FileRWMethod, blockSize int64) (ArchivedFiles, FileIds, error) {
	return buildInternal(path, method, blockSize, ALl)
}

func buildInternal(path string, method FileRWMethod, blockSize int64, eType uint16) (ArchivedFiles, FileIds, error) {
	dir, err := ioutil.ReadDir(path) //读取path下的所有目录和文件
	if err != nil {
		return nil, nil, err
	}

	//如有必要，构建合并文件
	//合并路径是路径中的子目录
	var (
		mergeFiles map[uint16]map[uint32]*DBFile
		mErr       error
	)

	for _, d := range dir { //这时候d是目录，dir是目录和文件的列表
		if d.IsDir() && strings.Contains(d.Name(), mergeDir) { //如果d 是目录并且目录名中包含mergeDir 说明这个目录是合并文件目录，继续递归
			mergePath := path + string(os.PathSeparator) + d.Name()                                    //将d下合并目录的路径拼接好
			if mergeFiles, _, mErr = buildInternal(mergePath, method, blockSize, eType); mErr != nil { //递归，寻找合并目录下的文件，因为buildInternal函数就是为了加载文件
				return nil, nil, mErr
			}
		}
	}

	fileIdsMap := make(map[uint16][]int) //fileIdsMap中放的是每种类型的文件id切片
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") { //看文件名中是否包含 .data 因为数据库的数据文件名中是包含.data的
			splitNames := strings.Split(d.Name(), ".") //将文件d名字用 “.”分割成切片列表  文件名格式是这样的 "000000002.data.str"
			id, _ := strconv.Atoi(splitNames[0])       //将字符串类型转换为int类型

			typ := DBFileSuffixName[splitNames[2]]
			if eType == ALl || typ == eType {
				fileIdsMap[typ] = append(fileIdsMap[typ], id)
			}
		}
	}

	//加载所有的db文件
	//每层目录下的每个类型都有一个活跃文件，每种类型都有自己单独的文件
	activeFileIds := make(FileIds)
	archFiles := make(ArchivedFiles)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs) //将文件id排序，活跃文件的id是最大的
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0

		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs)-1]) //活跃文件id

			length := len(fileIDs) - 1
			if strings.Contains(path, mergeDir) { //如果是合并目录中的文件的话  没有活跃文件，所以length要加一
				length++
			}
			for i := 0; i < length; i++ { //为每一个文件创建一个DBFile结构体
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files //不同类型的已归档文件
		activeFileIds[dataType] = activeFileId
	}

	//合并文件也是归档文件
	if mergeFiles != nil {
		for dType, file := range archFiles {
			if mergeFile, ok := mergeFiles[dType]; ok {
				for id, f := range mergeFile {
					file[id] = f
				}
			}
		}
	}
	return archFiles, activeFileIds, nil
}
