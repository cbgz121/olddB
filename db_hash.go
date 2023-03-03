package oldrosedb

import (
	"bytes"
	"oldrosedb/data-structer/hash"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"sync"
	"time"
)

//哈希索引
type HashIdx struct {
	mu      *sync.RWMutex
	indexes *hash.Hash
}

//创建一个新的哈希索引
func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New(), mu: new(sync.RWMutex)}
}

//HSet 将存储在 key 中的 hash 中的字段设置为 value。如果 key 不存在，则创建一个包含哈希的新 key
//如果字段已存在于哈希中，则将其覆盖
//返回指定键的哈希中的元素数
func (db *RoseDB) HSet(key, field, value interface{}) (res int, err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return -1, err
	}
	encField, err := utils.EncodeKey(field) // 之所以要把filed分开编码，是因为encode需要在很多地方调用，并不是所有的操作都有filed，所以encode中只封装了key，和value的编码
	if err != nil {
		return -1, err
	}

	if err = db.checkKeyValue(encKey, encVal); err != nil {
		return
	}
	if err = db.checkKeyValue(encField); err != nil {
		return
	}

	//如果存在值与设定值相同，则不执行任何操作
	oldVal := db.HGet(encKey, encField)
	if bytes.Compare(oldVal, encVal) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(encKey, encVal, encField, Hash, HashHSet)
	if err = db.store(e); err != nil { //存储在数据文件中
		return
	}

	res = db.hashIndex.indexes.HSet(string(encKey), string(encField), encVal) //存储在哈希索引中，更新哈希索引
	return
}

//HSetNx 将存储在键中的哈希中的字段设置为值，仅当字段尚不存在时
//如果 key 不存在，则创建一个包含哈希的新 key。如果字段已存在，则此操作无效
//操作成功则返回
func (db *RoseDB) HSetNx(key, field, value interface{}) (res int, err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return -1, err
	}

	encField, err := utils.EncodeKey(field)
	if err != nil {
		return -1, err
	}

	if err = db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	if err = db.checkKeyValue(encField, nil); err != nil {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(encKey), string(encField), encVal); res == 1 {
		e := storage.NewEntry(encKey, encVal, encField, Hash, HashHSet)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

//返回存储在key的哈希中的字段关联的值
func (db *RoseDB) HGet(key, field interface{}) []byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	encField, err := utils.EncodeKey(field)
	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HGet(string(encKey), string(encField))
}

//HGetAll 返回存储在 key 的哈希的所有字段和值
//在返回值中，每个字段名后面都有它的值，所以回复的长度是哈希大小的两倍
func (db *RoseDB) HGetAll(key interface{}) [][]byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HGetAll(string(encKey))
}

//HMSet 将多个哈希字段设置为多个值
func (db *RoseDB) HMSet(key interface{}, values ...interface{}) error {
	if len(values)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	var encVals [][]byte
	for i := 0; i < len(values); i++ { //将参数values提取出来并编码
		eval, err := utils.EncodeValue(values[i])
		if err != nil {
			return err
		}
		if err = db.checkKeyValue(encKey, eval); err != nil {
			return err
		}

		encVals = append(encVals, eval)
	}

	fields := make([]interface{}, 0) //从参数values中提取field
	for i := 0; i < len(values); i += 2 {
		fields = append(fields, encVals[i])
	}

	existVals := db.HMGet(encKey, fields...) //查看原本key中字段对应的值

	var encFields [][]byte
	for i := 0; i < len(fields); i++ { //将提取出来的field编码
		efields, err := utils.EncodeValue(fields[i])
		if err != nil {
			return err
		}
		if err = db.checkKeyValue(efields, nil); err != nil {
			return err
		}

		encFields = append(encFields, efields)
	}

	insertVals := make([][]byte, 0) //装待插入的字段和值

	if existVals == nil {
		//existsVals 表示key已过期
		insertVals = encVals
	} else {
		for i := 0; i < len(existVals); i++ {
			//如果存在值与设置值相同，则跳过该字段和值
			if bytes.Compare(encVals[i*2+1], existVals[i]) != 0 {
				insertVals = append(insertVals, encFields[i], encVals[i*2+1])
			}
		}
	}

	//检查所有字段和值
	if err := db.checkKeyValue(encKey, insertVals...); err != nil {
		return err
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for i := 0; i < len(insertVals); i += 2 {
		e := storage.NewEntry(encKey, insertVals[i+1], insertVals[i], Hash, HashHSet)
		if err := db.store(e); err != nil {
			return err
		}

		db.hashIndex.indexes.HSet(string(encKey), string(insertVals[i]), insertVals[i+1])
	}
	return nil
}

//HMGet 获取所有给定哈希字段的值
func (db *RoseDB) HMGet(key interface{}, fields ...interface{}) [][]byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	var encFileds [][]byte
	for i := 0; i < len(fields); i++ {
		efield, err := utils.EncodeValue(fields[i])
		if err != nil {
			return nil
		}

		if err = db.checkKeyValue(efield, nil); err != nil {
			return nil
		}

		encFileds = append(encFileds, efield)
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) { //位置不能变，因为访问过期数组时也会有并发问题
		return nil
	}

	values := make([][]byte, 0)
	for _, field := range encFileds {
		value := db.hashIndex.indexes.HGet(string(encKey), string(field))
		values = append(values, value)
	}

	return values
}

//HDel 从存储在 key 的哈希中删除指定的字段
//此哈希中不存在的指定字段将被忽略
//如果 key 不存在，则将其视为空哈希，此命令返回 false
func (db *RoseDB) HDel(key interface{}, fields ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}
	if fields == nil || len(fields) == 0 {
		return
	}

	var encFields [][]byte
	for i := 0; i < len(fields); i++ {
		var ef []byte
		if ef, err = utils.EncodeValue(fields[i]); err != nil {
			return
		}
		if err = db.checkKeyValue(ef, nil); err != nil {
			return
		}

		encFields = append(encFields, ef)
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range encFields {
		if ok := db.hashIndex.indexes.HDel(string(encKey), string(f)); ok == 1 {
			e := storage.NewEntry(encKey, nil, f, Hash, HashHDel)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

//如果key存在于哈希中，则 HKeyExists 返回
func (db *RoseDB) HKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return
	}
	return db.hashIndex.indexes.HKeyExists(string(encKey))
}

//如果字段是存储在 key 的哈希中的现有字段，则 HExists 返回
func (db *RoseDB) HExists(key, field interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	encField, err := utils.EncodeKey(field)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encField, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return
	}

	return db.hashIndex.indexes.HExists(string(encKey), string(encField))
}

//HLen 返回存储在 key 的哈希中包含的字段数
func (db *RoseDB) HLen(key interface{}) int {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return 0
	}

	return db.hashIndex.indexes.HLen(string(encKey))
}

//HKeys 返回存储在 key 的哈希中的所有字段名称
func (db *RoseDB) HKeys(key interface{}) (val []string) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}
	return db.hashIndex.indexes.HKeys(string(encKey))
}

//HVals 返回存储在 key 的散列中的所有值
func (db *RoseDB) HVals(key interface{}) (val [][]byte) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return nil
	}

	return db.hashIndex.indexes.HVals(string(encKey))
}

//HCear清除哈希中的键
func (db *RoseDB) HClear(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if !db.HKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, Hash, HashHClear)
	if err := db.store(e); err != nil {
		return err
	}

	db.hashIndex.indexes.HClear(string(encKey))
	delete(db.expires[Hash], string(encKey))
	return
}

//HExpire 设置哈希键的过期时间
func (db *RoseDB) HExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	if !db.HKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, Hash, HashHExpire)
	if err := db.store(e); err != nil {
		return err
	}

	db.expires[Hash][string(encKey)] = deadline
	return
}

//HTTL 返回key的剩余的存活时间
func (db *RoseDB) HTTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(encKey, Hash) {
		return
	}

	deadline, exist := db.expires[Hash][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
