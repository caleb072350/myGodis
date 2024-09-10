// 支持并发的哈希表

package dict

import (
	"sync"
	"sync/atomic"
)

type Dict struct {
	shards     []*Shard // 一个指针数组，其中每个元素为一个指向哈希表的指针，哈希表通过添加sync.RWMutex来实现并发访问
	shardCount int      // 哈希表的数量，其实就是数组的长度
	count      int32    //这里是Dict中存储的元素的数量

}

type Shard struct {
	table map[string]interface{}
	mutex sync.RWMutex
}

const (
	maxCapacity = 1 << 16
	minCapacity = 256
)

// return the mini power of two which is not less than cap
// 这个是什么鬼算法，没看懂
func computeCapacity(param int) (size int) {
	if param <= minCapacity {
		return minCapacity
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return maxCapacity
	}
	return n + 1
}

// 这个我理解就是go语言中的构造函数了，貌似经常看到
func Make(shardCountHint int) *Dict {
	shardCount := computeCapacity(shardCountHint)
	shards := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &Shard{table: make(map[string]interface{})}
	}
	return &Dict{shards: shards, shardCount: shardCount}
}

// 这个是个哈希计算方法，不用细纠结
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 通过这个函数计算得到key对应的map指针数组的index，即落在哪个位置的map上
func (d *Dict) spread(key string) int {
	h := int(fnv32(key))
	return (d.shardCount - 1) & h
}

// 现在元素直接存储在map中，没有用node拉链法，因此查找代码很简单
func (d *Dict) Get(key string) (val interface{}, exists bool) {
	shard := d.shards[d.spread(key)]
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	val, ok := shard.table[key]
	return val, ok
}

func (d *Dict) Len() int {
	return int(atomic.LoadInt32(&d.count))
}

// return the number of new inserted key-value
// 这里我猜作者是刚开始实现，比较基本的实现方法,插入也很简单
func (d *Dict) Put(key string, val interface{}) int {
	shard := d.shards[d.spread(key)]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	_, existed := shard.table[key]

	if existed {
		// update 目前是存在即不插入，后续根据不同的策略，实现插入逻辑
		return 0
	} else {
		// insert
		shard.table[key] = val
		atomic.AddInt32(&d.count, 1)
		return 1
	}
}

// 如果缺失就插入，返回1，否则返回0
func (d *Dict) PutIfAbsent(key string, val interface{}) int {
	shard := d.shards[d.spread(key)]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	_, existed := shard.table[key]
	if existed {
		return 0
	} else {
		shard.table[key] = val
		shard.table[key] = val
		return 1
	}
}

// 删除某个key，如果成功返回1，失败返回0
func (d *Dict) Remove(key string) int {
	shard := d.shards[d.spread(key)]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	_, existed := shard.table[key]
	if existed {
		delete(shard.table, key)
		atomic.AddInt32(&d.count, -1)
		return 1
	} else {
		return 0
	}
}
