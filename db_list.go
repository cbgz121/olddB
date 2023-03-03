package oldrosedb

import (
	"bytes"
	"oldrosedb/data-structer/list"
	"oldrosedb/storage"
	"oldrosedb/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

//list索引
type ListIdx struct {
	mu      *sync.RWMutex
	indexes *list.List
}

func newListIdx() *ListIdx {
	return &ListIdx{
		mu:      new(sync.RWMutex),
		indexes: list.New(),
	}
}

//将所有指定的值插入存储在 key 的列表的头部
//如果 key 不存在，则在执行push操作之前将其创建为空列表
func (db *RoseDB) LPush(key interface{}, values ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}

	var encVals [][]byte
	for i := 0; i < len(values); i++ {
		encVal, err := utils.EncodeValue(values[i])
		if err != nil {
			return -1, err
		}
		if err := db.checkKeyValue(encKey, encVal); err != nil {
			return -1, err
		}
		encVals = append(encVals, encVal)
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range encVals {
		e := storage.NewEntryNoExtra(encKey, val, List, ListLPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.LPush(string(encKey), val)
	}
	return
}

//在存储在 key 的列表的尾部插入所有指定的值
//如果 key 不存在，则在执行 push 操作之前将其创建为空列表
func (db *RoseDB) RPush(key interface{}, values ...interface{}) (res int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}

	var encVals [][]byte
	for i := 0; i < len(values); i++ {
		encVal, err := utils.EncodeValue(values[i])
		if err != nil {
			return -1, err
		}

		if err := db.checkKeyValue(encKey, encVal); err != nil {
			return -1, err
		}
		encVals = append(encVals, encVal)
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range encVals {
		e := storage.NewEntryNoExtra(encKey, val, List, ListRPush)
		if err = db.store(e); err != nil {
			return
		}
		res = db.listIndex.indexes.RPush(string(encKey), val)
	}
	return
}

//删除并返回存储在 key 的列表的第一个元素
func (db *RoseDB) LPop(key interface{}) ([]byte, error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return nil, ErrKeyExpired
	}

	val := db.listIndex.indexes.LPop(string(encKey))
	if val != nil {
		e := storage.NewEntryNoExtra(encKey, val, List, ListLPop)
		if err := db.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

//删除并返回存储在 key 的列表的最后一个元素
func (db *RoseDB) RPop(key interface{}) ([]byte, error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return nil, ErrKeyExpired
	}

	val := db.listIndex.indexes.RPop(string(encKey))
	if val != nil {
		e := storage.NewEntryNoExtra(encKey, val, List, ListRPop)
		if err := db.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

//返回列表 key 中，下标为 index 的元素
//索引是从零开始的，所以 0 表示第一个元素，1 表示第二个元素，依此类推
//负索引可用于指定从列表尾部开始的元素。这里，-1 表示最后一个元素，-2 表示倒数第二个，依此类推
//下标为0的一端为队头，一般删除都在队头，插入在队尾
func (db *RoseDB) LIndex(key interface{}, idx int) []byte {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(encKey, List) {
		return nil
	}

	return db.listIndex.indexes.LIndex(string(encKey), idx)
}

//根据参数 count 的值，移除列表中与参数 value 相等的元素
//count 的值可以是以下几种
//count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
//count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
//count = 0 : 移除表中所有与 value 相等的值
//这里不是清除key所以不用清除过期时间
func (db *RoseDB) LRem(key, value interface{}, count int) (int, error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return 0, nil
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return 0, nil
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return 0, ErrKeyExpired
	}

	res := db.listIndex.indexes.LRem(string(encKey), encVal, count)
	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(encKey, encVal, []byte(c), List, ListLRem)
		if err = db.store(e); err != nil {
			return res, err
		}
	}
	return res, nil
}

//在参考值枢轴之前或之后插入存储在key的列表中的元素
func (db *RoseDB) LInsert(key interface{}, option list.InsertOption, pivot, val interface{}) (count int, err error) {
	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return
	}
	envPivot, err := utils.EncodeValue(pivot)
	if err != nil {
		return
	}
	if err = db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	if strings.Contains(string(envPivot), ExtraSeparator) { //暂时不知道这一句的作用
		return 0, ErrExtraContainsSeparator
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	count = db.listIndex.indexes.LInsert(string(encKey), option, envPivot, encVal)
	if count != -1 {
		var buf bytes.Buffer
		buf.Write(envPivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry(encKey, encVal, buf.Bytes(), List, ListLInsert) //中间插入的元素带有额外信息
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

//将列表 key 下标为 index 的元素的值设置为 value
//返回是否成功
func (db *RoseDB) LSet(key interface{}, idx int, val interface{}) (ok bool, err error) {
	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return false, err
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return false, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if ok := db.listIndex.indexes.LSet(string(encKey), idx, encVal); ok {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(encKey, encVal, []byte(i), List, ListLSet)
		if err = db.store(e); err != nil {
			return false, err
		}
	}
	return
}

//返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定
//下标(index)参数 start 和 end 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推
//你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推 m
//超出范围的下标值不会引起错误
//如果 start 下标比列表的最大下标 end ( LLEN list 减去 1 )还要大，那么 LRANGE 返回一个空列表
//如果 stop 下标比 end 下标还要大，Redis将 stop 的值设置为 ends
func (db *RoseDB) LRange(key interface{}, start, end int) ([][]byte, error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(encKey, List) {
		return nil, ErrKeyExpired
	}

	return db.listIndex.indexes.LRange(string(encKey), start, end), nil
}

//对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
//这里不是清除key所以不用清除过期时间
func (db *RoseDB) LTrim(key interface{}, start, end int) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return ErrKeyExpired
	}

	if res := db.listIndex.indexes.LTrim(string(encKey), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(encKey, nil, buf.Bytes(), List, ListLTrim)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

//返回列表 key 的长度
//如果 key 不存在，则 key 被解释为一个空列表，返回 0
func (db *RoseDB) LLen(key interface{}) int {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return 0
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LLen(string(encKey))
}

//检查列表的键是否存在
func (db *RoseDB) LKeyExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(encKey, List) {
		return false
	}

	ok = db.listIndex.indexes.LKeyExists(string(encKey))
	return
}

//清除 List 的指定key
//这里因为直接是清除key，所以要清除过期时间
func (db *RoseDB) LClear(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if !db.LKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, List, ListLClear)
	if err = db.store(e); err != nil {
		return err
	}

	db.listIndex.indexes.LClear(string(encKey))
	delete(db.expires[List], string(encKey))
	return
}

//对指定的key设置过期时间
func (db *RoseDB) LExpire(key interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if !db.LKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, List, ListLExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[List][string(encKey)] = deadline
	return
}

//返回存活时间
func (db *RoseDB) LTTL(key interface{}) (ttl int64) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if db.checkExpired(encKey, List) {
		return
	}

	deadline, exist := db.expires[List][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
