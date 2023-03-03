package cache

import (
	"container/list"
	"sync"
)

type LruCache struct {
	capacity  int
	cacheMap  map[string]*list.Element
	cacheList *list.List
	mu        sync.Mutex
}

type lruItem struct {
	key   string
	value []byte
}

func NewLruCache(capacity int) *LruCache {
	lru := &LruCache{}
	if capacity > 0 {
		lru.cacheList = list.New()
		lru.cacheMap = make(map[string]*list.Element)
		lru.capacity = capacity
	}
	return lru
}

func (c *LruCache) Set(key, value []byte) {
	if c.capacity <= 0 || key == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(string(key), value)
}

func (c *LruCache) Get(key []byte) ([]byte, bool) {
	if c.capacity <= 0 || len(c.cacheMap) <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(string(key))
}

//这里的删除，实际上是缓存淘汰。即移除最近最少访问的节点（队首）
func (c *LruCache) RemoveOldest() {
	ele := c.cacheList.Back()
	if ele != nil {
		c.cacheList.Remove(ele)
		item := ele.Value.(*lruItem)
		delete(c.cacheMap, item.key)
	}
}

func (c *LruCache) Remove(key []byte) {
	if c.capacity <= 0 || len(c.cacheMap) <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.cacheMap[string(key)]; ok {
		delete(c.cacheMap, string(key))
		c.cacheList.Remove(ele)
	}
}

func (c *LruCache) get(key string) ([]byte, bool) {
	ele, ok := c.cacheMap[key]
	if ok {
		c.cacheList.MoveToFront(ele)
		item := ele.Value.(*lruItem)
		return item.value, true
	}
	return nil, false
}

func (c *LruCache) set(key string, value []byte) {
	ele, ok := c.cacheMap[key]
	if ok {
		c.cacheList.MoveToFront(ele) //这里是将list队首做队尾，队尾做队首
		item := c.cacheMap[key].Value.(*lruItem)
		item.value = value
	} else {
		ele = c.cacheList.PushFront(&lruItem{key: key, value: value})
		c.cacheMap[key] = ele

		if c.cacheList.Len() > c.capacity {
			c.RemoveOldest()
		}
	}
}
