package set

import (
	"oldrosedb/storage"
)

//Set是集合数据结构的实现
//集合只有键没有值,键就相当于集合的值

var existFlag = struct{}{} //相当于一个占位符，用来当作集合的值

type dumpFunc func(e *storage.Entry) error

type (
	//集合索引
	Set struct {
		record Record
	}

	//要保存的记录
	Record map[string]map[string]struct{}
)

//创建一个新的集合索引
func New() *Set {
	return &Set{make(Record)}
}

//迭代转储所有的键和值
func (s *Set) DumpIterate(fn dumpFunc) (err error) {
	for key, rec := range s.record {
		setKey := []byte(key)

		for member := range rec {
			ent := storage.NewEntryNoExtra(setKey, []byte(member), 3, 0)
			if err = fn(ent); err != nil {
				return
			}
		}
	}
	return
}

//将指定的成员添加到存储在 key 的集合中
//已经是该集合成员的指定成员将被忽略
//如果 key 不存在，则在添加指定成员之前创建一个新集合
func (s *Set) SAdd(key string, member []byte) int {
	if !s.exist(key) {
		s.record[key] = make(map[string]struct{})
	}

	s.record[key][string(member)] = existFlag
	return len(s.record[key])
}

//从 key 处的设置值存储中删除并返回一个或多个随机成员
func (s *Set) SPop(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count <= 0 {
		return val
	}

	for k := range s.record[key] {
		delete(s.record[key], k)
		val = append(val, []byte(k))

		count--
		if count == 0 {
			break
		}
	}
	return val
}

//如果 member 是存储在 key 的集合的成员，则返回
func (s *Set) SIsMember(key string, member []byte) bool {
	return s.fieldExist(key, string(member))
}

//仅使用 key 参数调用时，从存储在 key 的 set 值中返回一个随机元素
//如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合
//如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值
func (s *Set) SRandMember(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count == 0 {
		return val
	}

	if count >= len(s.record[key]) {
		for k := range s.record[key] {
			val = append(val, []byte(k))
		}
		return val
	}

	if count > 0 {
		for k := range s.record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.record[key] {
				return []byte(k)
			}
			return nil
		}
		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}
	return val
}

//从存储在 key 的集合中删除指定的成员
func (s *Set) SRem(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}

	if _, ok := s.record[key][string(member)]; ok {
		delete(s.record[key], string(member))
		return true
	}
	return false
}

//将成员从源集合移动到目标集合
//如果源集不存在或不包含指定元素，则不执行任何操作并返回0
func (s *Set) SMove(src, dst string, member []byte) bool {
	if !s.exist(src) {
		return false
	}
	if !s.exist(dst) {
		s.record[dst] = make(map[string]struct{})
	}
	if !s.fieldExist(src, string(member)) {
		return false
	}

	delete(s.record[src], string(member))
	s.record[dst][string(member)] = existFlag

	return true
}

//返回存储在 key 的集合的集合基数（元素数)
func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}
	return len(s.record[key])
}

//返回存储在key的集合中的所有成员
func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}
	for k := range s.record[key] {
		val = append(val, []byte(k))
	}
	return
}

//返回一个集合的全部成员，该集合是所有给定集合的并集
func (s *Set) SUnion(keys ...string) (val [][]byte) {
	m := make(map[string]bool)
	for _, k := range keys {
		if s.exist(k) {
			for v := range s.record[k] {
				m[v] = true
			}
		}
	}
	for v := range m {
		val = append(val, []byte(v))
	}
	return
}

//返回一个集合的全部成员，该集合是所有给定集合之间的差集，以第一个集合为基准
func (s *Set) SDiff(keys ...string) (val [][]byte) {
	if len(keys) == 0 || !s.exist(keys[0]) {
		return
	}

	//以第一个集合为基准用第一个集合中的成员，判断这个成员是否在其他集合中，不在则保存
	for v := range s.record[keys[0]] {
		flag := true
		for i := 1; i < len(keys); i++ {
			if s.SIsMember(keys[i], []byte(v)) {
				flag = false
				break
			}
		}
		if flag {
			val = append(val, []byte(v))
		}
	}
	return
}

//返回key是否存在集合中
func (s *Set) SKeyExists(key string) bool {
	return s.exist(key)
}

//从集合中清除指定的key
func (s *Set) SClear(key string) {
	if s.SKeyExists(key) {
		delete(s.record, key)
	}
}

//检查 set 的键是否存在
func (s *Set) exist(key string) bool {
	_, exist := s.record[key]
	return exist
}

//检查字段是否存于key中
func (s *Set) fieldExist(key, field string) bool {
	fields, exist := s.record[key]
	if !exist {
		return false
	}
	_, ok := fields[field]
	return ok
}
