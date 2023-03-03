package list

import (
	"container/list"
	"oldrosedb/storage"
	"reflect"
)

//List是双向链表的实现
//LInsert 的插入选项
type InsertOption uint8

type dumpFunc func(e *storage.Entry) error

const (
	//在枢轴之前插入
	Before InsertOption = iota
	//在枢轴之后插入
	After
)

type (
	//list索引
	List struct {
		//保存指定键的列表
		record Record
	}

	//列出要保存的记录
	Record map[string]*list.List
)

//创建一个新的链表索引
func New() *List {
	return &List{make(Record)}
}

//迭代转储的所有键和值
func (lis *List) DumpIterate(fn dumpFunc) (err error) {
	for key, l := range lis.record {
		listKey := []byte(key)

		for e := l.Front(); e != nil; e = e.Next() {
			value, _ := e.Value.([]byte)
			ent := storage.NewEntryNoExtra(listKey, value, 1, 1)
			if err = fn(ent); err != nil {
				return
			}
		}
	}
	return
}

//将所有指定的值插入存储在 key 的列表的头部
//如果 key 不存在，则在执行push操作之前将其创建为空列表
func (lis *List) LPush(key string, val ...[]byte) int {
	return lis.push(true, key, val...)
}

//删除并返回存储在 key 的列表的第一个元素
func (lis *List) LPop(key string) []byte {
	return lis.pop(true, key)
}

//在存储在 key 的列表的尾部插入所有指定的值
//如果 key 不存在，则在执行 push 操作之前将其创建为空列表
func (lis *List) RPush(key string, val ...[]byte) int {
	return lis.push(false, key, val...)
}

//删除并返回存储在 key 的列表的最后一个元素
func (lis *List) RPop(key string) []byte {
	return lis.pop(false, key)
}

func (lis *List) pop(front bool, key string) []byte {
	item := lis.record[key]
	var val []byte

	if item != nil && item.Len() > 0 { //item == nil 说明key列表没有初始化，item.len() == 0说明key列表没有存储元素
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}

		val = e.Value.([]byte)
		item.Remove(e)
	}
	return val
}

//返回列表 key 中，下标为 index 的元素
//索引是从零开始的，所以 0 表示第一个元素，1 表示第二个元素，依此类推
//负索引可用于指定从列表尾部开始的元素。这里，-1 表示最后一个元素，-2 表示倒数第二个，依此类推
//下标为0的一端为队头，一般删除都在队头，插入在队尾
func (lis *List) LIndex(key string, index int) []byte {
	ok, newIndex := lis.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex
	var val []byte
	e := lis.index(key, index)
	if e != nil {
		val = e.Value.([]byte)
	}
	return val
}

//根据参数 count 的值，移除列表中与参数 value 相等的元素
//count 的值可以是以下几种
//count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
//count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
//count = 0 : 移除表中所有与 value 相等的值
func (lis *List) LRem(key string, val []byte, count int) int {
	item := lis.record[key]
	if item == nil {
		return 0
	}

	var ele []*list.Element
	if count == 0 {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}
	if count > 0 {
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}
	if count < 0 {
		for p := item.Back(); p != nil && len(ele) < -count; p = p.Prev() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	for _, e := range ele {
		item.Remove(e)
	}
	length := len(ele)
	ele = nil

	return length
}

//在参考值枢轴之前或之后插入存储在key的列表中的元素
func (lis *List) LInsert(key string, option InsertOption, pivot, val []byte) int {
	e := lis.find(key, pivot)
	if e == nil {
		return -1
	}

	item := lis.record[key]
	if option == Before {
		item.InsertBefore(val, e)
	}
	if option == After {
		item.InsertAfter(val, e)
	}

	return item.Len()
}

//将列表 key 下标为 index 的元素的值设置为 value
func (lis *List) LSet(key string, index int, val []byte) bool {
	e := lis.index(key, index)
	if e == nil {
		return false
	}

	e.Value = val
	return true
}

//返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定
//下标(index)参数 start 和 end 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推
//你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推 m
//超出范围的下标值不会引起错误
//如果 start 下标比列表的最大下标 end ( LLEN list 减去 1 )还要大，那么 LRANGE 返回一个空列表
//如果 stop 下标比 end 下标还要大，Redis将 stop 的值设置为 ends
func (lis *List) LRange(key string, start, end int) [][]byte {
	var val [][]byte
	item := lis.record[key]

	if item == nil || item.Len() <= 0 {
		return val
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)

	if start > end || start >= length {
		return val
	}

	mid := length >> 1

	//从左向右遍历
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				val = append(val, p.Value.([]byte))
			}
		}
	} else { //从右向左遍历
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Next(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}
		if len(val) > 0 { //如果是从右向左遍历那么要调整val元素中的顺序以保证按照从start的顺序返回结果
			for i, j := 0, len(val)-1; i < j; i, j = i+1, j-1 {
				val[i], val[j] = val[j], val[i]
			}
		}
	}
	return val
}

//对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
func (lis *List) LTrim(key string, start, end int) bool {
	item := lis.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)

	//如果start小于0，end大于length - 1则直接返回，因为这时候所有的元素都在所要保留的区间里
	if start <= 0 && end >= length-1 {
		return false
	}

	//如果是这种情况则直接清空列表并返回
	if start > end || start >= length {
		lis.record[key] = nil
		return true
	}

	startEle, enfEle := lis.index(key, start), lis.index(key, end)
	if end-start+1 < (length >> 1) { //当要保留的区间长度小于原本列表长度的一半的话用这种方法效率较高
		newList := list.New()
		for p := startEle; p != enfEle.Next(); p = p.Next() {
			newList.PushBack(p.Value)
		}

		item = nil
		lis.record[key] = newList
	} else { //否则用这种方法效率较高
		var ele []*list.Element
		for p := item.Front(); p != enfEle; p = p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != startEle; p = p.Next() {
			ele = append(ele, p)
		}

		for _, e := range ele {
			item.Remove(e)
		}
		ele = nil
	}
	return true
}

//返回列表 key 的长度
//如果 key 不存在，则 key 被解释为一个空列表，返回 0
func (lis *List) LLen(key string) int {
	length := 0
	if lis.record[key] != nil {
		length = lis.record[key].Len()
	}
	return length
}

//清除 List 的指定key
func (lis *List) LClear(key string) {
	delete(lis.record, key)
}

//检查列表的键是否存在
func (lis *List) LKeyExists(key string) (ok bool) {
	_, ok = lis.record[key]
	return
}

//根据val找列表节点
func (lis *List) find(key string, val []byte) *list.Element {
	item := lis.record[key]
	var e *list.Element

	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				e = p
				break
			}
		}
	}
	return e
}

func (lis *List) index(key string, index int) *list.Element {
	ok, newIndex := lis.validIndex(key, index) //查询索引是否有效并返回新索引
	if !ok {
		return nil
	}

	index = newIndex
	item := lis.record[key]
	var e *list.Element

	if item != nil && item.Len() > 0 {
		//右移一位相当于除以二,这里因为列表是由双向链表实现的，所以查询时将链表从中间分为两部分，能加快查询速度
		if index <= (item.Len() >> 1) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i-- {
				val = val.Prev()
			}
			e = val
		}
	}
	return e
}

func (lis *List) push(front bool, key string, val ...[]byte) int {
	if lis.record[key] == nil {
		lis.record[key] = list.New()
	}

	for _, v := range val {
		if front {
			lis.record[key].PushFront(v)
		} else {
			lis.record[key].PushBack(v)
		}
	}
	return lis.record[key].Len()
}

//检查索引是否有效并返回新索引
func (lis *List) validIndex(key string, index int) (bool, int) {
	item := lis.record[key]
	if item == nil || item.Len() <= 0 {
		return false, index
	}

	length := item.Len()
	if index < 0 { //这里index如果是负数，说明想从列表尾部找元素，那么将这个负数与正数相加就是这个元素从头部开始数的位置
		index += length
	}
	return index >= 0 && index < length, index //这里，如果前面索引是负的经过转换后变成正的，然后返回转换后的索引,如果是正的则直接返回原索引
}

//处理 start 和 end 的值（负数和极端情况）
func (lis *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}

	if end < 0 {
		end += length
	}

	if start < 0 {
		start = 0
	}

	if end >= length {
		end = length - 1
	}

	return start, end
}
