// 支持并发的哈希表

package dict

import (
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table []*Shard
	count int32
}

type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

// 这个我理解就是go语言中的构造函数了，貌似经常看到
func MakeConcurrent(shardCount int) *ConcurrentDict {
	if shardCount < 1 {
		shardCount = 16
	}
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{m: make(map[string]interface{})}
	}
	d := &ConcurrentDict{
		count: 0,
		table: table,
	}
	return d
}

const prime32 = uint32(16777619)

// 这个是个哈希计算方法，不用细纠结
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 通过这个函数计算得到key对应的map指针数组的index，即落在哪个位置的map上
func (d *ConcurrentDict) spread(hashCode uint32) uint32 {
	if d == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(d.table))
	return (tableSize - 1) & hashCode
}

// 获取index位置上的Node头节点
func (d *ConcurrentDict) getShard(index uint32) *Shard {
	if d == nil {
		panic("dict is nil")
	}
	return d.table[index]
}

// 现在元素直接存储在map中，没有用node拉链法，因此查找代码很简单
func (d *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	shard := d.getShard(index)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	val, exists = shard.m[key]
	return
}

func (d *ConcurrentDict) Len() int {
	if d == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&d.count))
}

// return the number of new inserted key-value
func (d *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	shard := d.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	} else {
		shard.m[key] = val
		d.addCount()
		return 1
	}
}

// 如果缺失就插入，返回1，否则返回0
func (d *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	shard := d.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		return 0
	} else {
		shard.m[key] = val
		d.addCount()
		return 1
	}
}

// 如果存在就更新，否则就不更新
func (d *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	shard := d.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1
	} else {
		return 0
	}
}

// 删除某个key，如果成功返回1，失败返回0
func (d *ConcurrentDict) Remove(key string) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	shard := d.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		result = 1
	} else {
		result = 0
	}
	return
}

func (d *ConcurrentDict) addCount() int32 {

	return atomic.AddInt32(&d.count, 1)
}

/*
 * may not contain new entry inserted during traversal
 */
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}
	for _, shard := range dict.table {
		for key, value := range shard.m {
			shard.mutex.RLock()
			continues := consumer(key, value)
			shard.mutex.RUnlock()
			if !continues {
				return
			}
		}
	}
}
