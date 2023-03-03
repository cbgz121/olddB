package oldrosedb

import (
	"sync"
)

//LockMgr 是一个锁管理器，管理不同数据结构的读写操作
//也用来管理事务
type LockMgr struct {
	locks map[DataType]*sync.RWMutex
}

func newLockMgr(db *RoseDB) *LockMgr {
	locks := make(map[DataType]*sync.RWMutex)

	//存储不同数据类型的锁
	locks[String] = db.strIndex.mu
	locks[Hash] = db.hashIndex.mu
	locks[List] = db.listIndex.mu
	locks[Set] = db.setIndex.mu
	locks[ZSet] = db.zsetIndex.mu

	return &LockMgr{locks: locks}
}

//Lock 锁定 dTypes 的 rw 用于写入
func (lm *LockMgr) Lock(dTypes ...DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].Lock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].Unlock()
		}
	}

	return unLockFunc
}

//RLock 锁定 dTypes 的 rw 以供读取
func (lm *LockMgr) RLock(dTypes ...DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].RLock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].RUnlock()
		}
	}
	return unLockFunc
}
