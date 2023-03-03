package oldrosedb

import (
	"encoding/binary"
	"io"
	"oldrosedb/index"
	"oldrosedb/storage"
	"os"
	"sync"
)

const (
	txIdLen = 8
)

type (
	//Txn 是一个rosedb 事务
	//可以通过调用 Txn 开始读写事务，通过调用 TxnView 开始只读事务
	//事务将在 Txn 和 TxnView 方法中自动提交或回滚
	Txn struct {
		//事务id 是一个递增的数字来标记一个唯一的 tx,事务id不重复，一直累加，用过的事务id存储在文件中
		id uint64
		db *RoseDB
		wg *sync.WaitGroup

		//strEntries 是 String 的写入entry，它们不同于其他数据结构
		strEntries map[string]*storage.Entry

		//writeEntries 是 List、Hash、Set、ZSet 的写入entry
		writeEntries []*storage.Entry

		//存放的是要删除的entry在writeEntries中的下标，用于writeOthersEntrys写入db文件时，确保这个entry不会被写入文件中
		skipIds map[int]struct{}

		//存储Hash, ZSet类型的key在writeEntries中的坐标，以确保writeEntries中不会被添加重复的值
		keysMap map[string]int

		//指示在事务中将处理多少个数据结构
		dsState uint16

		isFinished bool
	}

	//TxnMeta 表示 tx 运行时的一些事务信息
	TxnMeta struct {
		//现在最大的事务id
		MaxTxId uint64

		//在活跃文件中提交的 tx id
		ActiveTxIds *sync.Map

		//保存已成功提交的事务 ID
		CommittedTxIds map[uint64]struct{}

		//用于保存提交的 tx id 的文件
		txnFile *TxnFile
	}

	//磁盘中的单个文件以保存已提交的事务 ID
	TxnFile struct {
		File   *os.File // 文件
		Offset int64    // 写入偏移量
	}
)

//Txn 执行包括读取和写入的事务。
//如果函数没有返回错误，则事务被提交
//如果返回错误，则整个事务回滚
func (db *RoseDB) Txn(fn func(tx *Txn) error) (err error) {
	if db.isClosed() {
		return ErrDBIsClosed
	}
	txn := db.NewTransaction()

	if err = fn(txn); err != nil {
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		txn.Rollback()
		return
	}
	return
}

//TxnView 执行一个事务，包括只读
//因为是读操作所以不需要提交事务
func (db *RoseDB) TxnView(fn func(tx *Txn) error) (err error) {
	if db.isClosed() {
		return ErrDBIsClosed
	}
	txn := db.NewTransaction()

	dTypes := []DataType{String, List, Hash, Set, ZSet}
	unlockFunc := txn.db.lockMgr.RLock(dTypes...) //锁定所有类型的读？
	defer unlockFunc()

	if err = fn(txn); err != nil {
		txn.Rollback()
		return
	}
	txn.finished()
	return
}

//NewTransaction 创建一个新事务，现在不支持事务并发执行
//所以只能同时开启一个读写事务
//对于只读事务，可以执行多个，任何写操作都会被省略
func (db *RoseDB) NewTransaction() *Txn {
	db.mu.Lock()
	defer func() {
		db.txnMeta.MaxTxId += 1 //这里是改变调用方内部的值
		db.mu.Unlock()
	}()

	return &Txn{ //这里改变的是返回过去的接收方的值
		id:         db.txnMeta.MaxTxId + 1,
		db:         db,
		wg:         new(sync.WaitGroup),
		strEntries: make(map[string]*storage.Entry),
		keysMap:    make(map[string]int),
		skipIds:    make(map[int]struct{}),
	}
}

//提交事务
func (tx *Txn) Commit() (err error) {
	if tx.db.isClosed() {
		return ErrDBIsClosed
	}
	defer tx.finished()

	if len(tx.strEntries) == 0 && len(tx.writeEntries) == 0 {
		return
	}

	dTypes := tx.getDTypes() //得到tx事务中所有的数据结构类型后对所有的类型加锁
	unlockFunc := tx.db.lockMgr.Lock(dTypes...)
	defer unlockFunc()

	//将entry写入db文件中
	var indexes []*index.Indexer
	if len(tx.strEntries) > 0 && len(tx.writeEntries) > 0 {
		tx.wg.Add(2)
		go func() {
			defer tx.wg.Done()
			if indexes, err = tx.writeStrEntries(); err != nil {
				return
			}
		}()

		go func() {
			defer tx.wg.Done()
			if err = tx.writeOtherEntries(); err != nil {
				return
			}
		}()
		tx.wg.Wait()
		if err != nil {
			return err
		}
	} else {
		if indexes, err = tx.writeStrEntries(); err != nil {
			return
		}
		if err = tx.writeOtherEntries(); err != nil {
			return err
		}
	}

	//同步 db 文件以实现事务持久性
	if tx.db.config.Sync {
		if err := tx.db.Sync(); err != nil {
			return err
		}
	}

	//标记事务已提交
	if err = tx.db.MarkCommit(tx.id); err != nil {
		return
	}

	//建立索引
	for _, idx := range indexes {
		if err = tx.db.buildIndex(tx.strEntries[string(idx.Meta.Key)], idx, false); err != nil {
			return
		}
	}
	for _, entry := range tx.writeEntries {
		if err = tx.db.buildIndex(entry, nil, false); err != nil {
			return
		}
	}
	return

}

func (tx *Txn) writeStrEntries() (indexes []*index.Indexer, err error) {
	if len(tx.strEntries) == 0 {
		return
	}

	for _, entry := range tx.strEntries {
		if err = tx.db.store(entry); err != nil {
			return
		}
		activeFile, err := tx.db.getActiveFile(String)
		if err != nil {
			return nil, err
		}

		//生成索引
		indexes = append(indexes, &index.Indexer{
			Meta: &storage.Meta{
				Key: entry.Meta.Key,
			},
			FileId: activeFile.Id,
			Offset: activeFile.Offset - int64(entry.Size()),
		})
	}
	return
}

func (tx *Txn) writeOtherEntries() (err error) {
	if len(tx.writeEntries) == 0 {
		return
	}

	for i, entry := range tx.writeEntries {
		if _, ok := tx.skipIds[i]; ok {
			continue
		}
		if err = tx.db.store(entry); err != nil {
			return
		}
	}
	return
}

//回滚完成的当前事务
func (tx *Txn) Rollback() {
	tx.finished()
}

//MarkCommit 将 tx id 写入 txn 文件
func (db *RoseDB) MarkCommit(txId uint64) (err error) {
	buf := make([]byte, txIdLen)
	binary.BigEndian.PutUint64(buf[:], txId)

	offset := db.txnMeta.txnFile.Offset
	_, err = db.txnMeta.txnFile.File.WriteAt(buf, offset)
	if err != nil {
		return
	}
	db.txnMeta.txnFile.Offset += int64(len(buf))

	if db.config.Sync {
		if err = db.txnMeta.txnFile.File.Sync(); err != nil {
			return
		}
	}
	return
}

//LoadTxnMeta 加载 txn 元信息，提交的 tx id
func LoadTxnMeta(path string) (txnMeta *TxnMeta, err error) {
	txnMeta = &TxnMeta{
		CommittedTxIds: make(map[uint64]struct{}),
		ActiveTxIds:    new(sync.Map),
	}

	var (
		file    *os.File
		maxTxId uint64
		stat    os.FileInfo
	)
	if file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return
	}
	if stat, err = file.Stat(); err != nil {
		return
	}
	txnMeta.txnFile = &TxnFile{
		File:   file,
		Offset: stat.Size(),
	}

	if txnMeta.txnFile.Offset > 0 {
		var offset int64
		for {
			buf := make([]byte, txIdLen)
			_, err = file.ReadAt(buf, offset)
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return
			}
			txId := binary.BigEndian.Uint64(buf)
			if txId > maxTxId { //循环加载最大事务id
				maxTxId = txId
			}
			txnMeta.CommittedTxIds[txId] = struct{}{}
			offset += txIdLen
		}
	}
	txnMeta.MaxTxId = maxTxId
	// file.Close()
	return
}

func (tx *Txn) finished() {
	tx.strEntries = nil
	tx.writeEntries = nil
	tx.skipIds = nil
	tx.keysMap = nil
	tx.isFinished = true
}

func (tx *Txn) putEntry(e *storage.Entry) (err error) {
	if e == nil {
		return
	}

	if tx.db.isClosed() {
		return ErrDBIsClosed
	}
	if tx.isFinished {
		return ErrTxIsFinished
	}

	switch e.GetType() {
	case String:
		tx.strEntries[string(e.Meta.Key)] = e
	default:
		tx.writeEntries = append(tx.writeEntries, e)
	}
	tx.setDsState(e.GetType())
	return
}

func (tx *Txn) setDsState(dType DataType) {
	tx.dsState = tx.dsState | (1 << dType)
}

func (tx *Txn) getDTypes() (dTypes []uint16) {
	//string
	if (tx.dsState&(1<<String))>>String == 1 {
		dTypes = append(dTypes, String)
	}
	//list
	if (tx.dsState&(1<<List))>>List == 1 {
		dTypes = append(dTypes, List)
	}
	//hash
	if (tx.dsState&(1<<Hash))>>Hash == 1 {
		dTypes = append(dTypes, Hash)
	}
	//set
	if (tx.dsState&(1<<Set))>>Set == 1 {
		dTypes = append(dTypes, Set)
	}
	//zset
	if (tx.dsState&(1<<ZSet))>>ZSet == 1 {
		dTypes = append(dTypes, ZSet)
	}
	return
}
