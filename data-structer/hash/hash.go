package hash

import (
	"oldrosedb/storage"
)

//哈希表的结构实现,哈希表的底层操作和结构实现

type dumpFunc func(e *storage.Entry) error //定义用于将entry合并时的函数

type (
	//哈希表结构
	Hash struct {
		record Record
	}

	//要保存的哈希记录
	Record map[string]map[string][]byte
)

//创建一个新的哈希结构
func New() *Hash {
	return &Hash{make(Record)}
}

//迭代转储所有的键和值
func (h *Hash) DumpIterate(fn dumpFunc) (err error) {
	for key, h := range h.record {
		hashKey := []byte(key)

		for field, value := range h { //转储的时候活跃文件中的entry也被转储了
			//HashSet
			ent := storage.NewEntry(hashKey, value, []byte(field), 2, 0)
			if err = fn(ent); err != nil {
				return err
			}
		}
	}
	return
}

//HSet 将存储在 key 的 hash 中的字段设置为 value。如果 key 不存在，则创建一个包含哈希的新 key
//如果字段已存在于哈希中，则将其覆盖
func (h *Hash) HSet(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	if h.record[key][field] != nil {
		//如果该字段存在，则覆盖它
		h.record[key][field] = value
	} else {
		//如果该字段不存在则创建
		h.record[key][field] = value
		res = 1
	}
	return
}

//仅当字段尚不存在时，HSetNx 才将存储在键中的哈希中的字段设置为值
//如果 key 不存在，则创建一个包含哈希的新 key。如果字段已存在，则此操作无效
//操作成功则返回
func (h *Hash) HSetNx(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
		return 1
	}
	return 0
}

//HGet 返回与存储在 key 的哈希中的字段关联的值
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

//HGetAll 返回存储在 key 的哈希的所有字段和值
//在返回值中，每个字段名后面都有它的值，所以回复的长度是哈希大小的两倍
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return

}

//HDel 从存储在 key 的哈希中删除指定的字段。此哈希中不存在的指定字段将被忽略
//如果 key 不存在，则将其视为空哈希，此命令返回 fals
func (h *Hash) HDel(key, field string) int {
	if !h.exist(key) {
		return 0
	}

	if _, exist := h.record[key][field]; exist {
		delete(h.record[key], field)
		return 1
	}
	return 0
}

//如果key存在于散列中，HKeyExists 返回
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

//如果字段是存储在 key 的哈希中的现有字段，则 HExists 返回
func (h *Hash) HExists(key, field string) (ok bool) {
	if !h.exist(key) {
		return
	}

	if _, exist := h.record[key][field]; exist {
		ok = true
	}
	return
}

//返回存储在 key 的哈希中包含的字段数。
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

//返回存储在 key 的哈希中的所有字段名称
func (h *Hash) HKeys(key string) (val []string) {
	if !h.exist(key) {
		return
	}
	for k := range h.record[key] {
		val = append(val, k)
	}
	return
}

//返回存储在 key 的散列中的所有值
func (h *Hash) HVals(key string) (val [][]byte) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return
}

//清除哈希中的key
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
