package oldrosedb

import (
	"io"
	"log"
	"oldrosedb/data-structer/list"
	"oldrosedb/index"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//定义数据结构类型
type DataType = uint16

//五种不同的数据类型，目前支持String、List、Hash、Set、Sorted Set
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

//String 类型的操作，将是 Entry 的一部分，其他四种类型相同
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)

//List的操作类型
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
	ListLClear
	ListLExpire
)

//Hash操作类型
const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

//Set操作类型
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

//有序集合操作类型
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
	ZSetZClear
	ZSetZExpire
)

//创建string索引
func (db *RoseDB) buildStringIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.strIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	case StringExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.strIndex.idxList.Remove(idx.Meta.Key)
		} else {
			db.expires[String][string(idx.Meta.Key)] = int64(entry.Timestamp)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}
	case StringPersist:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[String], string(idx.Meta.Key))
	}
}

//创建List索引
func (db *RoseDB) buildListIndex(entry *storage.Entry) {
	if db.listIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case ListLPush:
		db.listIndex.indexes.LPush(key, entry.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, entry.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, entry.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(entry.Meta.Key), list.InsertOption(opt), pivot, entry.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, entry.Meta.Value)
		}
	case ListLTrim:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(entry.Meta.Key), start, end)
		}
	case ListLExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.listIndex.indexes.LClear(key)
		} else {
			db.expires[List][key] = int64(entry.Timestamp)
		}
	case ListLClear:
		db.listIndex.indexes.LClear(key)
	}
}

//创建hash索引
func (db *RoseDB) buildHashIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(entry.Meta.Extra), entry.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(entry.Meta.Extra))
	case HashHClear:
		db.hashIndex.indexes.HClear(key)
	case HashHExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.hashIndex.indexes.HClear(key)
		} else {
			db.expires[Hash][key] = int64(entry.Timestamp)
		}
	}
}

//创建set索引
func (db *RoseDB) buildSetIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, entry.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, entry.Meta.Value)
	case SetSMove:
		extra := entry.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), entry.Meta.Value)
	case SetSClear:
		db.setIndex.indexes.SClear(key)
	case SetSExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.setIndex.indexes.SClear(key)
		} else {
			db.expires[Set][key] = int64(entry.Timestamp)
		}
	}
}

//创建zset索引
func (db *RoseDB) buildZsetIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(entry.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(entry.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(entry.Meta.Value))
	case ZSetZClear:
		db.zsetIndex.indexes.ZClear(key)
	case ZSetZExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.zsetIndex.indexes.ZClear(key)
		} else {
			db.expires[ZSet][key] = int64(entry.Timestamp)
		}
	}
}

//从db文件中加载String、List、Hash、Set、ZSet索引
func (db *RoseDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		go func(dType uint16) {
			defer wg.Done()

			//将活跃文件和归档文件都写入dbFile中,将文件id存入fileIds中
			//归档文件
			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			//活跃文件
			activeFile, err := db.getActiveFile(dType)
			if err != nil {
				log.Fatalf("active file is nil, the db can not open.[%+v]", err)
				return
			}
			dbFile[activeFile.Id] = activeFile
			fileIds = append(fileIds, int(activeFile.Id))

			//按指定顺序加载 db 文件
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:   e.Meta,
							FileId: fid,
							Offset: offset,
						}
						offset += int64(e.Size())
						// df.SetOffset(offset) 

						if len(e.Meta.Key) > 0 { //建立索引
							if err := db.buildIndex(e, idx, true); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}

							//保存活跃的 tx id
							if i == len(fileIds)-1 && e.TxId != 0 {
								db.txnMeta.ActiveTxIds.Store(e.TxId, struct{}{})
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
				//df.Close(false)
			}
			// for i := 0; i < len(fileIds); i++ {
			// 	fid := uint32(fileIds[i])
			// 	df := dbFile[fid]
			// 	df.Close(false)
			// }
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}