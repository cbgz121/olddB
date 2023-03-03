package oldrosedb

import (
	"bytes"
	"oldrosedb/index"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"sync"
	"time"
)

//string 索引
type StrIdx struct {
	mu      *sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{
		idxList: index.NewSkipList(), mu: new(sync.RWMutex),
	}
}

//设置key来保存字符串值。如果 key 已经拥有一个值，它会被覆盖
//在成功的 Set 操作时，任何先前与该key关联的生存时间都将被丢弃
func (db *RoseDB) Set(key, value interface{}) error {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}
	return db.setVal(encKey, encVal)
}

//SetNx 是“如果不存在则设置”的缩写，如果键不存在，则设置key以保存字符串值
//在这种情况下，它等于 Set。当key已经持有值时，不进行任何操作
func (db *RoseDB) SetNx(key, value interface{}) (ok bool, err error) {
	encKey, encValue, err := db.encode(key, value)
	if err != nil {
		return false, err
	}
	if exist := db.StrExists(encKey); exist {
		return
	}

	if err = db.Set(encKey, encValue); err == nil {
		ok = true
	}
	return
}

//SetEx 设置key以保存字符串值并在给定秒数后设置key超时
func (db *RoseDB) SetEx(key, value interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}

	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, encVal, deadline, String, StringExpire)
	if err := db.store(e); err != nil {
		return err
	}

	//设置字符串索引，存储在跳表中
	if err = db.setIndexer(e); err != nil {
		return
	}
	//设置过期时间
	db.expires[String][string(encKey)] = deadline
	return
}

//获取key的值。如果键不存在，则返回错误
func (db *RoseDB) Get(key, dest interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(encKey)
	if err != nil {
		return err
	}

	if len(val) > 0 {
		err = utils.DecodeValue(val, dest)
	}
	return err
}

//将 key 设置为 value 并返回存储在 key 中的旧值
func (db *RoseDB) GetSet(key, value, dest interface{}) (err error) {
	err = db.Get(key, dest)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return
	}
	return db.Set(key, value)
}

//将多个键设置为多个值
func (db *RoseDB) MSet(values ...interface{}) error {
	if len(values)&2 != 0 {
		return ErrWrongNumberOfArgs
	}

	var keys [][]byte
	var vals [][]byte

	for i := 0; i < len(values); i += 2 {
		encKey, encVal, err := db.encode(values[i], values[i+1])
		if err != nil {
			return err
		}
		if err := db.checkKeyValue(encKey, encVal); err != nil {
			return err
		}

		var skip bool
		if db.config.IdxMode == KeyValueMemMode {
			existVal, err := db.getVal(encKey)
			if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
				return err
			}

			//如果存在值与设置值相同，则跳过此键和值
			if bytes.Compare(existVal, encVal) == 0 {
				skip = true
			}
		}

		if !skip {
			keys = append(keys, encKey)
			vals = append(vals, encVal)
		}
	}
	if len(keys) == 0 || len(vals) == 0 {
		return nil
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(keys); i++ {
		e := storage.NewEntryNoExtra(keys[i], vals[i], String, StringSet)
		if err := db.store(e); err != nil {
			return err
		}
		//清除过期时间
		delete(db.expires[String], string(keys[i]))

		//设置String索引信息，存储在跳表中
		if err := db.setIndexer(e); err != nil {
			return err
		}

		//设置缓存信息
		db.cache.Set(keys[i], vals[i])
	}
	return nil
}

//获取所有给定键的值
func (db *RoseDB) MGet(keys ...interface{}) ([]interface{}, error) {
	var encKeys [][]byte
	for _, key := range keys {
		encKey, err := utils.EncodeKey(key)
		if err != nil {
			return nil, err
		}
		if err := db.checkKeyValue(encKey, nil); err != nil {
			return nil, err
		}

		encKeys = append(encKeys, encKey)
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	var vals []interface{}
	for _, encKey := range encKeys {
		val, err := db.getVal(encKey)
		if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

//如果 key 已经存在并且是一个字符串，则此命令将值附加到字符串的末尾
//如果 key 不存在，则创建它并将其设置为空字符串，因此 Append 在这种特殊情况下将类似于 Set
//值的原始类型必须是字符串，否则追加后会得到错误的值
func (db *RoseDB) Append(key interface{}, value string) (err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return err
	}

	var existVal []byte
	err = db.Get(key, &existVal)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return err
	}

	existVal = append(existVal, encVal...) //解包并存入
	return db.Set(encKey, existVal)
}

//删除存储在 key 中的值
func (db *RoseDB) Remove(key interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, String, StringRem)
	if err := db.store(e); err != nil {
		return err
	}

	db.strIndex.idxList.Remove(encKey)
	delete(db.expires[String], string(encKey))
	db.cache.Remove(encKey)
	return nil
}

//设置key的过期时间
func (db *RoseDB) Expire(key interface{}, duration int64) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if duration <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	var value []byte
	if value, err = db.getVal(encKey); err != nil {
		return err
	}

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, value, deadline, String, StringExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[String][string(encKey)] = deadline
	return
}

//存活时间
func (db *RoseDB) TTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline, exist := db.expires[String][string(encKey)] //获得过期时间
	if !exist {
		return
	}
	if expired := db.checkExpired(encKey, String); expired { //检查是否过期
		return
	}

	return deadline - time.Now().Unix() //如果没有过期，计算存活时间
}

//清除过期时间
func (db *RoseDB) Persist(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(encKey)
	if err != nil {
		return err
	}
	e := storage.NewEntryNoExtra(encKey, val, String, StringPersist)
	if err = db.store(e); err != nil {
		return
	}

	if err = db.setIndexer(e); err != nil {
		return
	}
	delete(db.expires[String], string(encKey))
	db.cache.Set(encKey, val)
	return
}

//检查key是否存在
func (db *RoseDB) StrExists(key interface{}) bool {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(encKey)
	if exist && !db.checkExpired(encKey, String) {
		return true
	}
	return false
}

func (db *RoseDB) setVal(key, value []byte) (err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	//如果存在值与设定值相同，则不执行任何操作,在KeyValueMemMode模式下
	//如果在KeyOnliyMemMode下不在判断，因为在KeyOnliyMemMode如果要判断的话要进入文件查看，效率太低
	if db.config.IdxMode == KeyValueMemMode {
		var existVal []byte
		existVal, err = db.getVal(key)
		if err != nil && err != ErrKeyExpired && err != ErrKeyNotExist {
			return
		}

		if bytes.Compare(existVal, value) == 0 {
			//清除过期时间
			if _, ok := db.expires[String][string(key)]; ok {
				delete(db.expires[String], string(key))
			}
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}

	//清除过期时间
	delete(db.expires[String], string(key))
	//设置String索引信息，存储在跳表中
	if err = db.setIndexer(e); err != nil {
		return err
	}

	//如果必要的话存储在缓存中
	db.cache.Set(key, value)
	return
}

func (db *RoseDB) setIndexer(e *storage.Entry) error {
	activeFile, err := db.getActiveFile(String)
	if err != nil {
		return err
	}
	//string索引，存储在跳表中
	idx := &index.Indexer{
		Meta: &storage.Meta{
			Key: e.Meta.Key,
		},
		FileId: activeFile.Id,
		Offset: activeFile.Offset - int64(e.Size()), //因为在设置索引之前entry已经存入了活跃文件中，所以这里计算偏移量时要用减
	}

	//在KeyValueMemMode模式下,key和value都会存储在内存中
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = e.Meta.Value
	}

	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return nil
}

func (db *RoseDB) getVal(key []byte) ([]byte, error) {
	//从内存中的跳表获得索引信息
	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}
	//类型断言
	idx, _ := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	//检查key是否过期
	if db.checkExpired(key, String) {
		return nil, ErrKeyExpired
	}

	//KeyValueMemMode模式下,value将会被存储在内存中
	//所以从索引信息中获取值
	if db.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}

	//在KeyOnlyMemMode模式下，value不在内存中
	//所以如果存在lru缓存中就从获得值
	//否则，根据offset从db文件中获取
	if db.config.IdxMode == KeyOnlyMemMode {
		//从缓存中获取
		if value, ok := db.cache.Get(key); ok {
			return value, nil
		}

		df, err := db.getActiveFile(String)
		if err != nil {
			return nil, err
		}
		if idx.FileId != df.Id { //如果活跃文件ID不等于key索引中存储的Field说明文件已经被存入归档文件当中
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		value := e.Meta.Value

		//将值存入缓存
		db.cache.Set(key, value)
		return value, nil
	}
	return nil, ErrKeyNotExist
}
