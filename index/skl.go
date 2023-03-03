package index

// SkipList is the implementation of skip list, skip list is an efficient data structure that can replace the balanced binary search tree.
// It can insert, delete, and query in O(logN) time complexity average.
// For a specific explanation of the skip list,  you can refer to Wikipedia: https://en.wikipedia.org/wiki/Skip_list.
//SkipList是skip list的实现，skip list是一种可以替代平衡二叉搜索树的高效数据结构
//它可以在 O(logN) 平均时间复杂度内插入、删除和查询
import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	// the max level of the skl indexes, can be adjusted according to the actual situation.
	//skl跳表的最高层数，可根据实际情况调整
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

// iterate the skl node, ends when the return value is false.
//迭代skl节点，返回值为false时结束
type handleEle func(e *Element) bool

type (
	Node struct {
		next []*Element //用于存储当前节点的每一层的下一个节点
	}

	// Element element is the data stored.
	//存储的数据
	Element struct {
		Node
		key   []byte
		value interface{}
	}

	// SkipList define the skip list.
	//跳表结构
	SkipList struct {
		Node
		maxLevel       int
		Len            int
		randSource     rand.Source
		probability    float64
		probTable      []float64
		prevNodesCache []*Node //用于存储待插入节点的每一层的前一个节点
	}
)

// NewSkipList create a new skip list.
//创建一个新的跳表
func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}

// Key the key of the Element.
//元素的键
func (e *Element) Key() []byte {
	return e.key
}

// Value the value of the Element.
//元素的值
func (e *Element) Value() interface{} {
	return e.value
}


//设置元素的值
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

//跳表的一级索引是有序的原始数据
//根据Next方法可以得到一个串联了所有数据的链表
func (e *Element) Next() *Element {
	return e.next[0]
}

//跳表的第一个元素
//获取skl的head元素，向后遍历获取所有数据
func (t *SkipList) Front() *Element {
	return t.next[0]
}

//将元素放入跳表，如果键已存在则替换值
func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := t.backNodes(key)

	//这里用 = 也是一样的因为经过上一个for循环的过滤 element.key 不可能比key小, 而且跳表也是有序递增排列的， 一定是大于等于key
	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, t.randomLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}

	t.Len++
	return element
}

// Get find value by the key, returns nil if not found.
//通过键查找值，如果没有找到则返回 nil
func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- { //当找到next.key > key 的element时继续在本节点node中往下寻找，因为有可能有更小的值
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 { //这里用 = 也是一样的因为经过上一个for循环的过滤 next.key 不可能比key小, 而且跳表也是有序递增排列的，一定是大于等于key
		return next
	}

	return nil
}

// Exist check if exists the key in skl.
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

// Remove element by the key.
func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)

	//这里用 = 也是一样的因为经过上一个for循环的过滤 element.key 不可能比key小, 而且跳表也是有序递增排列的，一定是大于等于key
	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}

		t.Len--
		return element
	}
	return nil
}

// Foreach iterate all elements in the skip list.
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

//在 key 处找到前一个节点
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node //头结点
	var next *Element

	prevs := t.prevNodesCache

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}

		prevs[i] = prev
	}

	return prevs
}

//找到与前缀匹配的第一个元素
func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil {
		next = t.Front()
	}

	return next
}

//func (r *Rand) Float64() float64
//返回一个取值范围在[0.0, 1.0)的伪随机float64值，所以不直接用Float64方法
//生成随机索引level
func (t *SkipList) randomLevel() (level int) {
	//返回一个int64类型的非负的63位伪随机数
	//对于需要随机指定位数的，当位数不够是，可以通过前边补1达到长度一致
	r := float64(t.randSource.Int63()) / (1 << 63)

	level = 1
	// 当 level < MAX_LEVEL，且随机数小于设定的晋升概率时，level + 1
	for level < t.maxLevel && r < t.probTable[level] {
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
