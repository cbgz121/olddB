package oldrosedb

import (
	"oldrosedb/data-structer/set"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"sync"
	"time"
)

type SetIdx struct {
	mu      *sync.RWMutex
	indexes *set.Set
}

//创建一个新的set索引
func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New(), mu: new(sync.RWMutex)}
}

//将指定的成员添加到存储在 key 的集合中
//已经是该集合成员的指定成员将被忽略
//如果 key 不存在，则在添加指定成员之前创建一个新集合
func (db *RoseDB) SAdd(key interface{}, members ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}

	var encMembers [][]byte
	for i := 0; i < len(members); i++ {
		eval, err := utils.EncodeValue(members[i])
		if err != nil {
			return -1, err
		}
		if err = db.checkKeyValue(encKey, eval); err != nil {
			return -1, err
		}
		encMembers = append(encMembers, eval)
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range encMembers {
		exist := db.setIndex.indexes.SIsMember(string(encKey), m)
		if !exist {
			e := storage.NewEntryNoExtra(encKey, m, Set, SetSAdd)
			if err = db.store(e); err != nil {
				return
			}
			res = db.setIndex.indexes.SAdd(string(encKey), m)
		}
	}
	return
}

//从 key 处的设置值存储中删除并返回一个或多个随机成员
func (db *RoseDB) SPop(key interface{}, count int) (values [][]byte, err error) {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return nil, err
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(encKey, Set) {
		return nil, ErrKeyExpired
	}

	values = db.setIndex.indexes.SPop(string(encKey), count)
	for _, v := range values {
		e := storage.NewEntryNoExtra(encKey, v, Set, SetSRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

//如果 member 是存储在 key 的集合的成员，则返回
func (db *RoseDB) SIsMember(key, member interface{}) bool {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return false
	}

	if err = db.checkKeyValue(encKey, encMember); err != nil {
		return false
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return false
	}
	return db.setIndex.indexes.SIsMember(string(encKey), encMember)
}

//仅使用 key 参数调用时，从存储在 key 的 set 值中返回一个随机元素
//如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合
//如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值
func (db *RoseDB) SRandMember(key interface{}, count int) [][]byte {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return nil
	}
	return db.setIndex.indexes.SRandMember(string(encKey), count)
}

//移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略
func (db *RoseDB) SRem(key interface{}, members ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return -1, err
	}

	var encMembers [][]byte
	for i := 0; i < len(members); i++ {
		eval, err := utils.EncodeValue(members[i])

		if err != nil {
			return -1, err
		}
		if err = db.checkKeyValue(encKey, eval); err != nil {
			return -1, err
		}

		encMembers = append(encMembers, eval)
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(encKey, Set) {
		return
	}

	for _, m := range encMembers {
		if ok := db.setIndex.indexes.SRem(string(encKey), m); ok {
			e := storage.NewEntryNoExtra(encKey, m, Set, SetSRem)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

//将成员从源集合移动到目标集合
//如果源集不存在或不包含指定元素，则不执行任何操作并返回0
func (db *RoseDB) SMove(src []byte, dst []byte, member interface{}) error {
	eval, err := utils.EncodeValue(member)

	if err != nil {
		return err
	}
	if err = db.checkKeyValue(nil, eval); err != nil {
		return err
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(src, Set) {
		return ErrKeyExpired
	}
	if db.checkExpired(dst, Set) {
		return ErrKeyExpired
	}

	if ok := db.setIndex.indexes.SMove(string(src), string(dst), eval); ok {
		e := storage.NewEntry(src, eval, dst, Set, SetSMove)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

//返回存储在 key 的集合的集合基数（元素数)
func (db *RoseDB) SCard(key interface{}) int {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return 0
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return 0
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return 0
	}
	return db.setIndex.indexes.SCard(string(encKey))
}

//返回存储在key的集合中的所有成员
func (db *RoseDB) SMembers(key interface{}) (val [][]byte) {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return nil
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return
	}
	return db.setIndex.indexes.SMembers(string(encKey))
}

//返回一个集合的全部成员，该集合是所有给定集合的并集
func (db *RoseDB) SUnion(keys ...interface{}) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	var encKeys [][]byte
	for i := 0; i < len(keys); i++ {
		encKey, err := utils.EncodeKey(keys[i])

		if err != nil {
			return nil
		}
		if err := db.checkKeyValue(encKey, nil); err != nil {
			return nil
		}
		encKeys = append(encKeys, encKey)
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range encKeys {
		if db.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SUnion(validKeys...)
}

//返回一个集合的全部成员，该集合是所有给定集合之间的差集，以第一个集合为基准
func (db *RoseDB) SDiff(keys ...interface{}) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	var encKeys [][]byte
	for i := 0; i < len(keys); i++ {
		encKey, err := utils.EncodeKey(keys[i])

		if err != nil {
			return nil
		}
		if err := db.checkKeyValue(encKey, nil); err != nil {
			return nil
		}

		encKeys = append(encKeys, encKey)
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range encKeys {
		if db.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SDiff(validKeys...)
}

//返回key是否存在集合中
func (db *RoseDB) SKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(encKey, Set) {
		return
	}

	ok = db.setIndex.indexes.SKeyExists(string(encKey))
	return
}

//从集合中清除指定的key
func (db *RoseDB) SClear(key interface{}) (err error) {
	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, Set, SetSClear)
	if err = db.store(e); err != nil {
		return
	}
	db.setIndex.indexes.SClear(string(encKey))
	return
}

//为集合中的key成员创建过期时间
func (db *RoseDB) SExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, Set, SetSExpire)
	if err = db.store(e); err != nil {
		return
	}
	db.expires[Set][string(encKey)] = deadline
	return
}

//返回存活时间
func (db *RoseDB) STTL(key interface{}) (ttl int64) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if db.checkExpired(encKey, Set) {
		return
	}

	deadline, exist := db.expires[Set][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
