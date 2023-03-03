package oldrosedb

import (
	"oldrosedb/storage"
	"time"
)

//数据索引模式
type DataIndexMode int

const (
	//键和值都存在内存中，在这种模式下读取操作会非常快
	//因为没有磁盘寻道，只是从内存中对应的数据结构中取值
	//该模式适用于数值比较小的场景
	KeyValueMemMode DataIndexMode = iota

	//KeyOnlyMemMode 只key入内存，取值时有磁盘寻道
	//因为值在磁盘上的 db 文件中
	KeyOnlyMemMode
)

const (
	//默认db服务器地址和端口
	DefaultAddr = "127.0.0.1:5200"

	//默认rosedb数据目录
	//使用时不要忘记更改路径
	DefaultDirPath = "/tmp/simpledb"

	//默认db文件大小为16MB
	//如果达到该大小，则永远不会打开 db 文件进行写入
	DefaultBlockSize = 16 * 1024 * 1024

	//默认key的大小为1MB
	DefaultMaxKeySize = uint32(1 * 1024 * 1024)

	//默认value的大小为8MB
	DefaultMaxValueSize = uint32(8 * 1024 * 1024)

	//默认磁盘文件回收阈值：64
	//这意味着当磁盘上至少有 64 个归档文件时，它将被回收
	DefaultMergeThreshold = 64

	//将根据检查间隔设置一个计时器
	//然后合并操作会定期执行
	DefaultMergeCheckInterval = time.Hour * 24

	//默认缓存容量：0，这意味着我们默认不使用它
	//只能在 KeyOnlyMemMode 中打开缓存，因为在此模式下值在磁盘中
	DefaultCacheCapacity = 0
)

//配置oldrosedb的开启选项
type Config struct {
	Addr         string               //服务器地址
	DirPath      string               //db文件的oldrosedb 目录路径
	BlockSize    int64                //每个db文件的大小
	RwMethod     storage.FileRWMethod //db 文件读写方法
	IdxMode      DataIndexMode        //数据索引模式
	MaxKeySize   uint32
	MaxValueSize uint32

	//Sync 是否将写入从 OS 缓冲区缓存同步到实际磁盘
	//如果为 false，并且机器崩溃，那么最近的一些写入可能会丢失
	//请注意，如果只是进程崩溃（而机器没有），则不会丢失任何写入
	//默认值为false
	Sync               bool
	MergeThreshold     int //回收磁盘的阈值
	MergeCheckInterval time.Duration
	CacheCapacity      int
}

//默认配置
func DefaultConfig() Config {
	return Config{
		Addr:               DefaultAddr,
		DirPath:            DefaultDirPath,
		BlockSize:          DefaultBlockSize,
		RwMethod:           storage.FileIO,
		IdxMode:            KeyOnlyMemMode,
		MaxKeySize:         DefaultMaxKeySize,
		MaxValueSize:       DefaultMaxValueSize,
		Sync:               false,
		MergeThreshold:     DefaultMergeThreshold,
		MergeCheckInterval: DefaultMergeCheckInterval,
		CacheCapacity:      DefaultCacheCapacity,
	}
}
