package oldrosedb

import (
	"oldrosedb/data-structer/zset"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"sync"
	"time"
)

//zset索引
type ZsetIdx struct {
	mu      *sync.RWMutex
	indexes *zset.SortedSet
}

//创建一个新的zset索引
func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{
		mu:      new(sync.RWMutex),
		indexes: zset.New(),
	}
}

//将一个或多个 member 元素及其 score 值加入到有序集 key 当中
func (db *RoseDB) ZAdd(key interface{}, score float64, member interface{}) error {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return err
	}

	//如果key和member对应的score已经存在，则什么都不做
	if ok, oldScore := db.ZScore(encKey, encMember); ok && oldScore == score {
		return nil
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntry(encKey, encMember, extra, ZSet, ZSetZAdd)
	if err := db.store(e); err != nil {
		return err
	}

	db.zsetIndex.indexes.ZAdd(string(encKey), score, string(encMember))
	return nil
}

//返回 key 处排序集中成员的分数
func (db *RoseDB) ZScore(key, member interface{}) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return false, -1
	}
	if db.checkExpired(encKey, ZSet) {
		return
	}

	return db.zsetIndex.indexes.ZScore(string(encKey), string(encMember))
}

//返回有序集 key 的基数
func (db *RoseDB) ZCard(key interface{}) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	if db.checkExpired(encKey, ZSet) {
		return 0
	}

	return db.zsetIndex.indexes.ZCard(string(encKey))
}

//返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列
//排名以 0 为底，也就是说， score 值最小的成员排名为 0
func (db *RoseDB) ZRank(key, member interface{}) int64 {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return -1
	}

	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRank(string(encKey), string(encMember))
}

// 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。
// 排名以 0 为底，也就是说， score 值最大的成员排名为 0
func (db *RoseDB) ZRevRank(key, member interface{}) int64 {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return -1
	}
	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRevRank(string(encKey), string(encMember))
}

// 为有序集 key 的成员 member 的 score 值加上增量 increment 。
// 可以通过传递一个负数值 increment ，让 score 减去相应的值 。
// 当 key 不存在，或 member 不是 key 的成员时， ZIncrBy 等同于 ZAdd
func (db *RoseDB) ZIncrBy(key interface{}, increment float64, member interface{}) (float64, error) {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return increment, err
	}
	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return increment, err
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	increment = db.zsetIndex.indexes.ZIncrBy(string(encKey), increment, string(encMember))

	extra := utils.Float64ToStr(increment)
	e := storage.NewEntry(encKey, encMember, []byte(extra), ZSet, ZSetZAdd)
	if err := db.store(e); err != nil {
		return increment, err
	}

	return increment, nil
}

// 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递增(从小到大)来排序
func (db *RoseDB) ZRange(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRange(string(encKey), start, stop)
}

// 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递增(从小到大)来排序
//可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回
func (db *RoseDB) ZRangeWithScores(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRangeWithScores(string(encKey), start, stop)
}

//返回有序集 key 中，指定区间内的成员。
//其中成员的位置按 score 值递减(从大到小)来排列
//以原跳表尾部第一个元素下标为0开始，也就是说以跳表最后一个元素为第一个元素
func (db *RoseDB) ZRevRange(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRange(string(encKey), start, stop)
}

//以原跳表尾部第一个元素下标为0开始，也就是说以跳表最后一个元素为第一个元素
func (db *RoseDB) ZRevRangeWithScores(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRangeWithScores(string(encKey), start, stop)
}

//移除有序集 key 中的一个或多个成员，不存在的成员将被忽略
func (db *RoseDB) ZRem(key, member interface{}) (ok bool, err error) {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return
	}
	if err = db.checkKeyValue(encKey, encMember); err != nil {
		return
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if db.checkExpired(encKey, ZSet) {
		return
	}

	if ok = db.zsetIndex.indexes.ZRem(string(encKey), string(encMember)); ok {
		e := storage.NewEntryNoExtra(encKey, encMember, ZSet, ZSetZRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

//根据排名获取member及分值信息，从小到大排列遍历，即分值最低排名为0，依次类推
//最低的rank为0，依此类推
func (db *RoseDB) ZGetByRank(key interface{}, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZGetByRank(string(encKey), rank)
}

//根据排名获取member及分值信息，从大到小排列遍历
func (db *RoseDB) ZRevGetByRank(key interface{}, rank int) []interface{} {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevGetByRank(string(encKey), rank)
}

//返回排序集中在 key 处的所有元素，其分数在 min 和 max 之间（包括分数等于 min 或 max 的元素）
//元素被认为从低到高排序
func (db *RoseDB) ZScoreRange(key interface{}, min, max float64) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZScoreRange(string(encKey), min, max)
}

//返回排序集中在 key 处的所有元素，其分数在 max 和 min 之间（包括分数等于 max 或 min 的元素
//与排序集的默认排序相反，对于此命令，元素被认为是从高到低排序的
func (db *RoseDB) ZRevScoreRange(key interface{}, max, min float64) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevScoreRange(string(encKey), max, min)
}

// 检查key是否在有序集合中
func (db *RoseDB) ZKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return
	}

	ok = db.zsetIndex.indexes.ZKeyExists(string(encKey))
	return
}

// 从集合中清除key
func (db *RoseDB) ZClear(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if !db.ZKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, ZSet, ZSetZClear)
	if err = db.store(e); err != nil {
		return
	}
	db.zsetIndex.indexes.ZClear(string(encKey))
	return
}

//设置过期时间
func (db *RoseDB) ZExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if !db.ZKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, ZSet, ZSetZExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[ZSet][string(encKey)] = deadline
	return
}

//计算存活时间
func (db *RoseDB) ZTTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if !db.ZKeyExists(encKey) {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	deadline, exist := db.expires[ZSet][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
