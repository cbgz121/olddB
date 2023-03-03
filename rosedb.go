package oldrosedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"oldrosedb/cache"
	"oldrosedb/index"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	//自定义错误
	//key为空
	ErrEmptyKey = errors.New("oldrosedb: the key is empty")

	//key不存在
	ErrKeyNotExist = errors.New("oldrosedb: key not exist")

	//key超长
	ErrKeyTooLarge = errors.New("oldrosedb: key exceeded the max length")

	//value超长
	ErrValueTooLarge = errors.New("oldrosedb: value exceeded the max length")

	//索引为空
	ErrNilIndexer = errors.New("oldrosedb: indexer is nil")

	//extra包含分隔符
	ErrExtraContainsSeparator = errors.New("oldrosedb: extra contains separator \\0")

	//ttl无效
	ErrInvalidTTL = errors.New("oldrosedb: invalid ttl")

	//key已过期
	ErrKeyExpired = errors.New("oldrosedb: key is expired")

	//合并和单次合并不能同时执行
	ErrDBisMerging = errors.New("oldrosedb: can`t do reclaim and single reclaim at the same time")

	//db关闭后无法使用
	ErrDBIsClosed = errors.New("oldrosedb: db is closed, reopen it")

	//tx 完成
	ErrTxIsFinished = errors.New("oldrosedb: transaction is finished, create a new one")

	//活跃文件为nil
	ErrActiveFileIsNil = errors.New("oldrosedb: active file is nil")

	//参数数量错误
	ErrWrongNumberOfArgs = errors.New("oldrosedb: wrong number of arguments")
)

const (

	//rosedb配置文件保存路径
	configSaveFile = string(os.PathSeparator) + "DB.CFG"

	//rosedb事务元信息保存路径
	dbTxMetaSaveFile = string(os.PathSeparator) + "DB.TX.META"

	//rosedb reclaim path，临时目录，reclaim后会被移除
	mergePath = string(os.PathSeparator) + "oldrosedb_merge"

	//额外信息的分隔符，一些命令不能包含它
	ExtraSeparator = "\\0"

	//不同数据结构的数量，现在有五个（string，list，hash，set，zset)
	DataStructureNum = 5
)

type (
	//rosedb 结构，代表一个数据库实例
	RoseDB struct {
		//不同数据类型的当前活动文件，存储如下：map[DataType]*storage.DBFile
		activeFile *sync.Map
		archFiles  storage.ArchivedFiles //归档文件
		strIndex   *StrIdx               //String 索引
		listIndex  *ListIdx              //List 索引
		hashIndex  *HashIdx              //Hash索引
		setIndex   *SetIdx               //Set 索引
		zsetIndex  *ZsetIdx              //  有序集合索引
		config     Config                //oldrosedb 的配置信息
		mu         sync.RWMutex          //mutex
		expires    Expires               //过期目录
		lockMgr    *LockMgr              //lockMgr 控制读写隔离
		txnMeta    *TxnMeta              //事务中使用的 Txn 元信息
		closed     uint32
		cache      *cache.LruCache //lru缓存
		isMerging  bool
	}

	//保存不同key的过期信息
	Expires map[DataType]map[string]int64
)

func Open(config Config) (*RoseDB, error) {
	//如果不存在则创建目录路径
	if !utils.DirExist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//从磁盘加载数据文件
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//设置活跃文件进行写入
	activeFiles := new(sync.Map)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles.Store(dataType, file) //将文件映射入activeFiles Map中，dataType是键，file是值
	}

	//加载事务元信息
	txnMeta, err := LoadTxnMeta(config.DirPath + dbTxMetaSaveFile)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeFile: activeFiles,
		archFiles:  archFiles,
		config:     config,
		strIndex:   newStrIdx(),
		listIndex:  newListIdx(),
		hashIndex:  newHashIdx(),
		setIndex:   newSetIdx(),
		zsetIndex:  newZsetIdx(),
		expires:    make(Expires),
		txnMeta:    txnMeta,
		cache:      cache.NewLruCache(config.CacheCapacity),
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}
	db.lockMgr = newLockMgr(db)

	//从db文件加载索引
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	//后台打开协程合并文件
	go db.handleMerge()
	return db, nil
}

func (db *RoseDB) encode(key, value interface{}) (encKey, encVal []byte, err error) {
	if encKey, err = utils.EncodeKey(key); err != nil {
		return
	}
	if encVal, err = utils.EncodeValue(value); err != nil {
		return
	}
	return
}

//关闭数据库并保存相关配置
func (db *RoseDB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err = db.saveConfig(); err != nil {
		return err
	}

	// 关闭并且同步活跃文件
	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Close(true); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}

	//关闭归档文件
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err = file.Close(true); err != nil {
				return err
			}
		}
	}

	atomic.StoreUint32(&db.closed, 1)
	return
}

//检查键值是否合法
func (db *RoseDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}
	return nil
}

//关闭数据库之前保存配置
func (db *RoseDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile //创建一个文件将配置信息序列化装入文件中
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()

	return
}

//将entry写入db文件中
func (db *RoseDB) store(e *storage.Entry) error {
	//如果文件大小不够，持久化 db 文件，并打开一个新的 db 文件
	config := db.config
	activeFile, err := db.getActiveFile(e.GetType()) //从内存中找到对应类型的活跃文件
	if err != nil {
		return err
	}

	if activeFile.Offset+int64(e.Size()) > config.BlockSize { //如果活跃文件所剩空间不能装下新的entry
		if err := activeFile.Sync(); err != nil {
			return err
		}

		//将旧的活跃db文件保存为归档文件,归档文件是保存在内存中的，打开数据库时加载
		activeFiledId := activeFile.Id
		db.archFiles[e.GetType()][activeFiledId] = activeFile

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFiledId+1, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}

		activeFile = newDbFile
	}

	//将entry写入db文件中
	if err := activeFile.Write(e); err != nil {
		return err
	}
	//更新\重新映射类型和活跃文件的关系，每个类型的文件都对应一个活跃文件，本句是更新内存中的活跃文件
	db.activeFile.Store(e.GetType(), activeFile)

	//根据配置决定是否持久化db文件
	if config.Sync {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

//将 db 文件持久化到稳定的存储
func (db *RoseDB) Sync() (err error) {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.activeFile.Range(func(key, value interface{}) bool { //将activeFile中的所有文件都进行同步一下
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Sync(); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}
	return
}

//Merge 将重新组织磁盘中的 db 文件的数据，删除无用的数据
//对于List、Hash、Set和ZSet，我们将内存中的数据直接dump(转储)到db文件中，所以会阻塞一段时间的读写。
//对于String，我们选择不同的方式来做同样的事情：按顺序加载db文件中的所有数据，并比较内存中的最新值，找到有效数据，并将它们重写到新的db文件中
//所以不会这样阻塞读写
func (db *RoseDB) Merge() (err error) {
	if db.isMerging {
		return ErrDBisMerging
	}

	db.mu.Lock()
	defer func() {
		db.isMerging = false
		db.mu.Unlock()
	}()
	db.isMerging = true

	//创建一个临时目录来存储新的db文件
	mergePath := db.config.DirPath + mergePath
	if !utils.DirExist(mergePath) {
		if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
			return err
		}
	}
	defer db.removeTempFiles(mergePath)

	wg := new(sync.WaitGroup)
	wg.Add(DataStructureNum)
	//将 List、Hash、Set、ZSet 直接转储到 db 文件
	go db.dump(wg)
	//合并字符串（比较和重写)
	go db.mergeString(wg)
	wg.Wait()
	return
}

func (db *RoseDB) dump(wg *sync.WaitGroup) {
	path := db.config.DirPath + mergePath
	for i := List; i < DataStructureNum; i++ {
		switch i {
		case List:
			go db.dumpInternal(wg, path, List)
		case Hash:
			go db.dumpInternal(wg, path, Hash)
		case Set:
			go db.dumpInternal(wg, path, Set)
		case ZSet:
			go db.dumpInternal(wg, path, ZSet)
		}
	}
}

func (db *RoseDB) dumpInternal(wg *sync.WaitGroup, path string, eType DataType) {
	defer wg.Done()

	cfg := db.config
	if len(db.archFiles[eType])+1 < cfg.MergeThreshold { //加一是因为把活跃文件也算进去了
		return
	}

	unLockFunc := db.lockMgr.Lock(eType)
	defer unLockFunc()

	var mergeFiles []*storage.DBFile
	//创建并存储第一个用于合并的 db 文件
	file, err := storage.NewDBFile(path, 0, cfg.RwMethod, cfg.BlockSize, eType)
	if err != nil {
		log.Printf("[dumpInternal]create new db file err.[%+v]\n", err)
		return
	}
	mergeFiles = append(mergeFiles, file)

	dumpStoreFn := func(e *storage.Entry) (err error) {
		if err = db.dumpStore(&mergeFiles, path, e); err != nil {
			log.Printf("[dumpInternal]store entry err.[%+v]\n", err)
		}
		return
	}

	//如果没有错误，则转储所有值并删除原始数据库文件
	var dumpErr error
	switch eType {
	case List:
		dumpErr = db.listIndex.indexes.DumpIterate(dumpStoreFn)
	case Hash:
		dumpErr = db.hashIndex.indexes.DumpIterate(dumpStoreFn)
	case Set:
		dumpErr = db.setIndex.indexes.DumpIterate(dumpStoreFn)
	case ZSet:
		dumpErr = db.zsetIndex.indexes.DumpIterate(dumpStoreFn)
	}
	if dumpErr != nil {
		return
	}

	for _, f := range db.archFiles[eType] {
		f.Close(false)           //关闭文件并且不进行持久化操作
		os.Remove(f.File.Name()) //删除原始数据库文件
	}

	value, ok := db.activeFile.Load(eType)
	if ok && value != nil { //转储的时候活跃文件中的entry也被转储了，所以这里可以直接将活跃文件删除
		activeFile, _ := value.(*storage.DBFile)

		if activeFile != nil { //活跃文件持久化到磁盘上并删除活跃文件
			activeFile.Close(true) //这里要持久化到磁盘上，因为如果不持久化直接删除活跃文件的话，未持久化的数据之后写入磁盘因为活跃文件已经被删除，所以会出现错误
			os.Remove(activeFile.File.Name())
		}
	}

	//将临时文件复制为新的数据库文件
	for _, f := range mergeFiles {
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[eType], f.Id)
		os.Rename(path+name, cfg.DirPath+name)
	}

	//重新加载数据文件
	if err = db.loadDBFiles(eType); err != nil {
		log.Printf("[dumpInternal]load db files err.[%+v]\n", err)
		return
	}
}

func (db *RoseDB) dumpStore(mergeFiles *[]*storage.DBFile, mergePath string, e *storage.Entry) (err error) {
	var df *storage.DBFile
	df = (*mergeFiles)[len(*mergeFiles)-1]
	cfg := db.config

	if df.Offset+int64(e.Size()) > cfg.BlockSize {
		if err = df.Sync(); err != nil {
			return
		}
		df, err = storage.NewDBFile(mergePath, df.Id+1, cfg.RwMethod, cfg.BlockSize, e.GetType())
		if err != nil {
			return
		}
		*mergeFiles = append(*mergeFiles, df)
	}

	if err = df.Write(e); err != nil {
		return
	}
	return
}

func (db *RoseDB) mergeString(wg *sync.WaitGroup) {
	defer wg.Done()

	if len(db.archFiles[0]) < db.config.MergeThreshold { //这里为什么不加一
		return
	}

	cfg := db.config
	path := db.config.DirPath + mergePath

	//加载合并文件
	//加载path合并目录下的文件
	//mergedFiles刚开始是为空的
	mergedFiles, _, err := storage.BuildType(path, cfg.RwMethod, cfg.BlockSize, String)
	if err != nil {
		log.Printf("[mergeString]build db file err.[%+v]", err)
		return
	}

	//archFiles里装的是加载出来的文件以及它的id，id为键，文件为值
	archFiles := mergedFiles[String]
	if archFiles == nil {
		archFiles = make(map[uint32]*storage.DBFile)
	}

	var (
		df        *storage.DBFile
		maxFileId uint32
		fileIds   []int
	)
	for fid := range archFiles {
		if fid > maxFileId {
			maxFileId = fid
		}
	}

	//跳过合并的文件
	for fid := range db.archFiles[String] {
		if _, exist := archFiles[fid]; !exist {
			fileIds = append(fileIds, int(fid))
		}
	}

	//按顺序合并文件
	sort.Ints(fileIds)
	for _, fid := range fileIds {
		dbFile := db.archFiles[String][uint32(fid)]
		validEntries, err := dbFile.FindValidEntries(db.validEntry)
		if err != nil && err != io.EOF {
			log.Printf(fmt.Sprintf("find valid entries err.[%+v]", err))
			return
		}

		//重写有效entry
		for _, ent := range validEntries {
			if df == nil || int64(ent.Size())+df.Offset > cfg.BlockSize {
				df, err = storage.NewDBFile(path, maxFileId, cfg.RwMethod, cfg.BlockSize, String)
				if err != nil {
					log.Printf(fmt.Sprintf("create db file err.[%+v]", err))
					return
				}
				db.archFiles[String][maxFileId] = df
				archFiles[maxFileId] = df
				maxFileId += 1
			}

			if err = df.Write(ent); err != nil {
				log.Printf(fmt.Sprintf("rewrite entry err.[%+v]", err))
				return
			}

			//更新索引
			item := db.strIndex.idxList.Get(ent.Meta.Key)
			if item != nil {
				idx, _ := item.Value().(*index.Indexer)
				if idx != nil {
					idx.Offset = df.Offset - int64(ent.Size())
					idx.FileId = df.Id
					db.strIndex.idxList.Put(idx.Meta.Key, idx)
				}
			}
		}

		//删除旧的db文件
		_ = dbFile.Close(false)
		_ = os.Remove(dbFile.File.Name())
	}

	for _, file := range archFiles {
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], file.Id)
		_ = os.Rename(path+name, cfg.DirPath+name) //改名并移动,意思就是直接重命名路径，直接更改路径
	}

	//重新加载db文件
	if err = db.loadDBFiles(String); err != nil {
		log.Printf("load db files err.[%+v]", err)
		return
	}
}

//为不同的数据结构创建索引
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer, isOpen bool) (err error) {
	if db.config.IdxMode == KeyValueMemMode && entry.GetType() == String {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	//事务中未提交的条目无效
	if entry.TxId != 0 && isOpen {
		if entry.TxId > db.txnMeta.MaxTxId {
			db.txnMeta.MaxTxId = entry.TxId
		}
		if _, ok := db.txnMeta.CommittedTxIds[entry.TxId]; !ok {
			return
		}
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	case storage.List:
		db.buildListIndex(entry)
	case storage.Hash:
		db.buildHashIndex(entry)
	case storage.Set:
		db.buildSetIndex(entry)
	case storage.ZSet:
		db.buildZsetIndex(entry)
	}
	return
}

func (db *RoseDB) getActiveFile(dType DataType) (file *storage.DBFile, err error) {
	//open打开数据库的时候会加载所有类型的活跃文件，所以这里是默认能找到活跃文件的
	value, ok := db.activeFile.Load(dType)
	if !ok || value == nil {
		return nil, ErrActiveFileIsNil
	}

	var typeOk bool
	if file, typeOk = value.(*storage.DBFile); !typeOk {
		return nil, ErrActiveFileIsNil
	}
	return
}

func (db *RoseDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	deadline, exist := db.expires[String][string(e.Meta.Key)]
	now := time.Now().Unix()
	if exist && deadline > now { //exist 为真并且deadline > now 说明key还没有过期
		return true
	}

	if e.GetMark() == StringSet || e.GetMark() == StringPersist {
		node := db.strIndex.idxList.Get(e.Meta.Key)
		if node == nil {
			return false
		}
		indexer, _ := node.Value().(*index.Indexer)
		if indexer != nil && bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
			if indexer.FileId == fileId && indexer.Offset == offset {
				return true
			}
		}
	}
	return false
}

//复制数据库目录进行备份
func (db *RoseDB) Backup(dir string) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if utils.DirExist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}
	return
}

func (db *RoseDB) loadDBFiles(eType DataType) error {
	cfg := db.config
	archivedFiles, activeIds, err := storage.BuildType(cfg.DirPath, cfg.RwMethod, cfg.BlockSize, eType)
	if err != nil {
		return err
	}
	db.archFiles[eType] = archivedFiles[eType]

	activeFile, err := storage.NewDBFile(cfg.DirPath, activeIds[eType], cfg.RwMethod, cfg.BlockSize, eType)
	if err != nil {
		return err
	}
	db.activeFile.Store(eType, activeFile)
	return nil
}

func (db *RoseDB) handleMerge() {
	//NewTicker 返回一个新的 Ticker，其中包含一个通道，该通道将在每次滴答后发送通道上的时间
	//这个的作用是定时合并文件
	ticker := time.NewTicker(db.config.MergeCheckInterval)
	defer ticker.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for range ticker.C {
		select {
		case <-sig:
			//当这个管道收到信号时，会清理无用的合并文件
			path := db.config.DirPath + mergePath
			db.removeTempFiles(path)
			return
		default:
			err := db.Merge()
			if err != nil && err != ErrDBisMerging {
				log.Printf("[handleMerge]db merge err.[%+v]", err)
			}
		}
	}
}

//检查key是否过期，如果需要删除它
func (db *RoseDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}

	if time.Now().Unix() > deadline { //对比现在的时间与过期时间
		expired = true

		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem) //创建一个没有额外信息的entry,为了删除过期的key，过期的key都会创建一个没有额外信息的entry，代表key已经被删除
			db.strIndex.idxList.Remove(key)                          //将key从索引中删除
		case List:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			db.listIndex.indexes.LClear(string(key))
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
			db.hashIndex.indexes.HClear(string(key))
		case Set:
			e = storage.NewEntryNoExtra(key, nil, Set, SetSClear)
			db.setIndex.indexes.SClear(string(key))
		case ZSet:
			e = storage.NewEntryNoExtra(key, nil, ZSet, ZSetZClear)
			db.zsetIndex.indexes.ZClear(string(key))
		}
		if err := db.store(e); err != nil { //将 e 存入文件
			log.Println("checkExpired: store entry err: ", err)
			return
		}
		//删除key中存储的过期信息
		delete(db.expires[dType], string(key))
	}
	return
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *RoseDB) removeTempFiles(path string) {
	infos, _ := ioutil.ReadDir(path)
	var str int
	if infos != nil {
		for _, info := range infos {
			if strings.Contains(info.Name(), "str") {
				str++
			} else {
				_ = os.Remove(info.Name())
			}
		}
	}
	if str == 0 { //string类型的文件不删除吗
		_ = os.RemoveAll(path) //这句意思是如果里面没有string类型的文件则直接把path这个目录删除
	}
}
