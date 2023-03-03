package zset

import (
	"oldrosedb/storage"
	"oldrosedb/utils"
	"math"
	"math/rand"
)

//zset实现

const (
	maxLevel    = 32
	probability = 0.25
)

type dumpFunc func(e *storage.Entry) error

type (
	//有序集合结构体
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	//有序集合节点
	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}

	sklLevel struct {
		forward *sklNode
		span    uint64
	}

	sklNode struct {
		member   string
		score    float64
		backward *sklNode
		level    []*sklLevel
	}

	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int64
		level  int16
	}
)

func New() *SortedSet {
	return &SortedSet{
		make(map[string]*SortedSetNode),
	}
}

//迭代转储所有的键和值
func (z *SortedSet) DumpIterate(fn dumpFunc) (err error) {
	for key, ss := range z.record {
		zsetKey := []byte(key)

		for e := ss.skl.head; e != nil; e = e.level[0].forward {
			extra := []byte(utils.Float64ToStr(e.score))
			// ZSet, ZSetZAdd
			ent := storage.NewEntry(zsetKey, []byte(e.member), extra, 4, 0)
			if err = fn(ent); err != nil {
				return
			}
		}
	}
	return
}

//将一个或多个 member 元素及其 score 值加入到有序集 key 当中
//如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该 member 在正确的位置上
func (z *SortedSet) ZAdd(key string, score float64, member string) {
	if !z.exist(key) {
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		z.record[key] = node
	}

	item := z.record[key]
	v, exist := item.dict[member]

	var node *sklNode
	if exist {
		if score != v.score {
			item.skl.sklDelete(v.score, member)
			node = item.skl.sklInsert(score, member)
		}
	} else {
		node = item.skl.sklInsert(score, member)
	}

	if node != nil {
		item.dict[member] = node
	}
}

//返回 key 处排序集中成员的分数
func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	if !z.exist(key) {
		return
	}

	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}
	return true, node.score
}

//返回有序集 key 的基数
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	return len(z.record[key].dict)
}

//返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列
//排名以 0 为底，也就是说， score 值最小的成员排名为 0
func (z *SortedSet) ZRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)
	rank-- //头结点不算入排名

	return rank
}

// 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。
// 排名以 0 为底，也就是说， score 值最大的成员排名为 0
func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)

	return z.record[key].skl.length - rank //技巧，用跳表总长减去当前排名就是倒叙后的排名
}

func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

// 为有序集 key 的成员 member 的 score 值加上增量 increment 。
// 可以通过传递一个负数值 increment ，让 score 减去相应的值。
// 当 key 不存在，或 member 不是 key 的成员时， ZIncrBy 等同于 ZAdd
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}

	z.ZAdd(key, increment, member)
	return increment
}

// 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递增(从小到大)来排序
//可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回
func (z *SortedSet) ZRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(stop), false, false)
}

// 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递增(从小到大)来排序
//可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回
func (z *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, true)
}

//返回有序集 key 中，指定区间内的成员。
//其中成员的位置按 score 值递减(从大到小)来排列
//以原跳表尾部第一个元素下标为0开始，也就是说以跳表最后一个元素为第一个元素
func (z *SortedSet) ZRevRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, false)
}

//以原跳表尾部第一个元素下标为0开始，也就是说以跳表最后一个元素为第一个元素
func (z *SortedSet) ZRevRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, true)
}

//移除有序集 key 中的一个或多个成员，不存在的成员将被忽略
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}

	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}

	return false
}

//根据排名获取member及分值信息，从小到大排列遍历，即分值最低排名为0，依次类推
//最低的rank为0，依此类推
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return nil
	}

	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

//按排名获取key的成员，从高到低排序
func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}

//返回排序集中在 key 处的所有元素，其分数在 min 和 max 之间（包括分数等于 min 或 max 的元素）
//元素被认为从低到高排序
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward //这个值如果不为nil那么一定是大于等于min的
	for p != nil {
		if p.score > max { //循环结束条件之一，若p迭代到值大于max说明范围搜索已经结束
			break
		}

		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}
	return
}

//返回排序集中在 key 处的所有元素，其分数在 max 和 min 之间（包括分数等于 max 或 min 的元素
//与排序集的默认排序相反，对于此命令，元素被认为是从高到低排序的
func (z *SortedSet) ZRevScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}

	for p != nil {
		if p.score < min { //循环结束条件之一，若p迭代到值小于min说明范围搜索已经结束
			break
		}

		val = append(val, p.member, p.score)
		p = p.backward
	}
	return
}

// 检查key是否在有序集合中
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// 从集合中清除key
func (z *SortedSet) ZClear(key string) {
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}

func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {

	skl := z.record[key].skl
	if rank < 0 || rank >= skl.length {
		return "", math.MinInt64
	}

	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}

	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}

	node := z.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}

	return node.member, node.score
}

func (z *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	skl := z.record[key].skl
	length := skl.length

	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += length
	}

	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *sklNode
	if reverse { //反转输出
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			//通过rank（跨度）查找start所在的元素，start是下标，下标加一就是到达start的所需的rank
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
	return
}

func (skl *skipList) sklDelete(score float64, member string) {
	update := make([]*sklNode, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- { //得到前驱节点
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		update[i] = p
	}

	p = p.level[0].forward
	if p != nil && score == p.score && p.member == member {
		skl.sklDeleteNode(p, update)
		return
	}
}

func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	updates := make([]*sklNode, maxLevel)
	rank := make([]uint64, maxLevel)

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- { //更新不超过跳表当前高度部分的前驱节点和前驱跨度（头结点到达前驱节点的跨度）
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if p.level[i] != nil {
			for p.level[i].forward != nil &&
				(p.level[i].forward.score < score ||
					(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
				rank[i] += p.level[i].span //前驱跨度值更新（头结点到达前驱节点的跨度）
				p = p.level[i].forward
			}
		}
		updates[i] = p
	}

	level := randomLevel()
	if level > skl.level {
		for i := skl.level; i < level; i++ { //更新超过跳表当前高度的部分的前驱节点和前驱跨度（头结点到达前驱节点的跨度）
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length) //超过当前跳表高度的部分的头结点跨度赋值为当前跳表长度
		}
		skl.level = level
	}

	p = sklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i]) //更新当前节点到下一个节点的跨度值
		updates[i].level[i].span = (rank[0] - rank[i]) + 1               //更新前驱节点到当前节点的跨度
	}

	for i := level; i < skl.level; i++ { //这里是当要插入的节点的level 小于当前跳表高度时，更新当前节点的前驱节点的跨度
		updates[i].level[i].span++
	}

	//更新backward指向
	if updates[0] == skl.head { //如果当前节点的第一层的前驱节点是头结点，那么当前节点的backward是nil，说明当前节点是跳表的第一个节点
		p.backward = nil
	} else {
		p.backward = updates[0]
	}

	//更新tail指向
	if p.level[0].forward != nil {
		p.level[0].forward.backward = p
	} else {
		skl.tail = p
	}

	skl.length++
	return p
}

func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		if updates[i].level[i].forward == p {
			updates[i].level[i].span += p.level[i].span - 1 //更新前驱节点的跨度值 因为将p删除所以用p的跨度值减一
			updates[i].level[i].forward = p.level[i].forward
		} else {
			updates[i].level[i].span-- //如果前驱节点的下一个节点不是p则直接将跨度值减一
		}
	}

	if p.level[0].forward != nil { //说明p不是最后一个节点，那么要修改p的下一个节点的backward指向
		p.level[0].forward.backward = p.backward
	} else {
		skl.tail = p.backward //如果p是最后一个节点，那么要修改tail的指向为p的前驱节点
	}

	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil { //若跳表的最高层只有头节点了，那么修改level的值
		skl.level--
	}

	skl.length-- //删除一个节点，跳表的长度减一
}

func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(sklLevel) //初始化之后 node.level就不为nil了
	}
	return node
}

func newSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	var level int16 = 1
	for float32(rand.Int31()&0xFFFF) < (probability * 0xFFFF) {
		level++
	}

	if level < maxLevel {
		return level
	}
	return maxLevel
}

func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) { //这里和插入的时候不一样，若和p相等也进入循环
			rank += p.level[i].span
			p = p.level[i].forward
		}
		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	var traversed uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- { //根据所给的rank找到对应的成员，找到rank等于所给rank的成员
		for p.level[i].forward != nil && (traversed+p.level[i].span) <= rank {
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank {
			return p
		}
	}
	return nil
}
