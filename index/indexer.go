package index

import "oldrosedb/storage"

//数据索引信息，存储在跳表中
type Indexer struct {
	Meta   *storage.Meta //额外数据信息,key,value
	FileId uint32        //存储数据的文件id
	Offset int64         //entry数据查询起始位置
}
