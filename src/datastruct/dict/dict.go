// 支持并发的哈希表

package dict

import (
	"sync"
	"sync/atomic"
)

type Dict struct {
	// shards     []*Shard // 一个指针数组，其中每个元素为一个指向哈希表的指针，哈希表通过添加sync.RWMutex来实现并发访问
	table       atomic.Value // 通过atomic.Value 的 Store 和 Load 来实现原子操作 table本质上也是[]*Shardj
	nextTable   []*Shard     // 在rehash过程中使用
	nextTableMu sync.Mutex   // nextTable的互斥锁，用来实现对nextTable的单一进程访问
	count       int32        // 这里是Dict中存储的元素的数量
	rehashIndex int32        //在rehash过程中正在处理的[]*Shard的index，小于这个index的已经放到nextTable中，大于这个index的还未处理，等于这个index的正在处理中

}

// 数据节点，每个Shard位置用Node拉链式来表示该index位置处的所有数据
type Node struct {
	key      string
	val      interface{}
	next     *Node
	hashCode uint32
}

type Shard struct {
	head  *Node
	mutex sync.RWMutex
}

const (
	maxCapacity      = 1 << 15
	minCapacity      = 16
	rehashConcurrent = 4 // 在rehash过程中的并发度，这个并发是线程并发还是进程并发？待进一步阅读源码
	loadFactor       = 0.75
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
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{}
	}
	d := &Dict{
		count:       0,
		rehashIndex: -1,
	}
	d.table.Store(table)
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
func (d *Dict) spread(hashCode uint32) uint32 {
	if d == nil {
		panic("dict is nil")
	}
	table, _ := d.table.Load().([]*Shard)
	tableSize := uint32(len(table))
	return (tableSize - 1) & hashCode
}

// 获取index位置上的Node头节点
func (d *Dict) getShard(index uint32) *Shard {
	if d == nil {
		panic("dict is nil")
	}
	table, ok := d.table.Load().([]*Shard)
	if !ok {
		panic("dict is nil")
	}
	return table[index]
}

// 获取nextTable上的index位置处的Node节点
func (d *Dict) getNextShard(hashCode uint32) *Shard {
	if d == nil {
		panic("dict is nil")
	}
	if d.nextTable == nil {
		panic("next table is nil")
	}
	nextTableSize := uint32(len(d.nextTable))
	index := (nextTableSize - 1) & uint32(hashCode)
	return d.nextTable[index]
}

// 确保nextTable生成完毕
func (d *Dict) ensureNextTable() {
	if d.nextTable == nil {
		d.nextTableMu.Lock()

		// check-lock-check
		if d.nextTable == nil {
			table, _ := d.table.Load().([]*Shard)
			tableSize := uint32(len(table))
			// init nextTable
			nextShardCount := tableSize << 1
			if nextShardCount > maxCapacity || nextShardCount < tableSize {
				nextShardCount = maxCapacity
			}
			if nextShardCount <= tableSize {
				// reach limit, cannot resize
				atomic.StoreInt32(&d.rehashIndex, -1)
				return // 生成失败，将rehashIndex置为-1
			}
			nextTable := make([]*Shard, nextShardCount)
			var i uint32
			for i = 0; i < nextShardCount; i++ {
				nextTable[i] = &Shard{}
			}
			d.nextTable = nextTable
		}

		d.nextTableMu.Unlock()
	}
}

// 遍历链表，得到某个key
func (shard *Shard) Get(key string) (val interface{}, exist bool) {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	node := shard.head
	for node != nil {
		if node.key == key {
			return node.val, true
		}
		node = node.next
	}
	// not found
	return nil, false
}

// 现在元素直接存储在map中，没有用node拉链法，因此查找代码很简单
func (d *Dict) Get(key string) (val interface{}, exists bool) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	rehashIndex := atomic.LoadInt32(&d.rehashIndex)
	if rehashIndex >= int32(index) {
		/* if rehashIndex > index, then the shard has finished resize, put in the next table
		 * if rehashIndex == index, then the shard is being resized or just finished.
		 * Resizing will not be finished until the lock has been released.
		 */
		d.ensureNextTable() // 确保nextTable生成了
		shard := d.getNextShard(hashCode)
		val, exists = shard.Get(key)
	} else {
		shard := d.getShard(index)
		val, exists = shard.Get(key)
	}
	return
}

func (d *Dict) Len() int {
	if d == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&d.count))
}

// 往链表中插入元素
func (shard *Shard) Put(key string, val interface{}, hashCode uint32) int {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	if node == nil {
		// empty shard
		node = &Node{
			key:      key,
			val:      val,
			hashCode: hashCode,
		}
		shard.head = node
		return 1
	} else {
		for {
			if node.key == key {
				// update
				node.val = val
				return 0
			}
			if node.next == nil {
				// insert
				node.next = &Node{
					key:      key,
					val:      val,
					hashCode: hashCode,
				}
				return 1
			}
			node = node.next
		}
	}
}

// return the number of new inserted key-value
func (d *Dict) Put(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	rehashIndex := atomic.LoadInt32(&d.rehashIndex)
	if rehashIndex >= int32(index) {
		d.ensureNextTable()
		shard := d.getNextShard(hashCode)
		result = shard.Put(key, val, hashCode)
	} else {
		shard := d.getShard(index)
		result = shard.Put(key, val, hashCode)
	}
	if result == 1 {
		d.addCount()
	}
	return
}

func (shard *Shard) PutIfAbsent(key string, val interface{}, hashCode uint32) int {
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	if node == nil {
		// empty shard
		node = &Node{
			key:      key,
			val:      val,
			hashCode: hashCode,
		}
		shard.head = node
		return 1
	} else {
		for {
			if node.key == key {
				// already exists
				return 0
			}
			if node.next == nil {
				// insert
				node.next = &Node{
					key:      key,
					val:      val,
					hashCode: hashCode,
				}
				return 1
			}
			node = node.next
		}
	}
}

// 如果缺失就插入，返回1，否则返回0
func (d *Dict) PutIfAbsent(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)

	rehashIndex := atomic.LoadInt32(&d.rehashIndex)
	if rehashIndex >= int32(index) {
		d.ensureNextTable()
		shard := d.getNextShard(hashCode)
		result = shard.PutIfAbsent(key, val, hashCode)
	} else {
		shard := d.getShard(index)
		result = shard.PutIfAbsent(key, val, hashCode)
	}
	if result == 1 {
		d.addCount()
	}
	return
}

func (shard *Shard) PutIfExists(key string, val interface{}) int {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	for node != nil {
		if node.key == key {
			node.val = val
			return 1
		}
		node = node.next
	}
	return 0
}

// 如果存在就更新，否则就不更新
func (d *Dict) PutIfExists(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)

	rehashIndex := atomic.LoadInt32(&d.rehashIndex)
	if rehashIndex >= int32(index) {
		d.ensureNextTable()
		shard := d.getNextShard(hashCode)
		result = shard.PutIfExists(key, val)
	} else {
		shard := d.getShard(index)
		result = shard.PutIfExists(key, val)
	}
	return
}

// 从链表删除某个key
func (shard *Shard) Remove(key string) int {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	node := shard.head
	if node == nil {
		return 0
	} else if node.key == key {
		shard.head = node.next
		return 1
	} else {
		for {
			if node.next == nil {
				return 0
			}
			if node.next.key == key {
				node.next = node.next.next
				return 1
			}
			node = node.next
		}
	}
}

// 删除某个key，如果成功返回1，失败返回0
func (d *Dict) Remove(key string) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := d.spread(hashCode)
	rehashIndex := atomic.LoadInt32(&d.rehashIndex)
	if rehashIndex >= int32(index) {
		d.ensureNextTable()
		shard := d.getNextShard(index)
		result = shard.Remove(key)
	} else {
		shard := d.getShard(index)
		result = shard.Remove(key)
	}
	if result > 0 {
		atomic.AddInt32(&d.count, -1)
	}
	return
}

func (d *Dict) addCount() int32 {
	count := atomic.AddInt32(&d.count, 1)
	table, _ := d.table.Load().([]*Shard)
	if float64(count) >= float64(len(table))*loadFactor {
		d.rehash()
	}
	return count
}

func (d *Dict) rehash() {
	if !atomic.CompareAndSwapInt32(&d.rehashIndex, -1, 0) {
		// rehash is already in progress
		return
	}
	d.ensureNextTable()

	var wg sync.WaitGroup
	wg.Add(rehashConcurrent)
	for i := 0; i < rehashConcurrent; i++ {
		go d.transfer(&wg) // 开启多个协程 加速rehash过程
	}
	wg.Wait()

	// finish rehash
	d.table.Store(d.nextTable)
	d.nextTable = nil
	atomic.StoreInt32(&d.rehashIndex, -1)
}

func (d *Dict) transfer(wg *sync.WaitGroup) {
	table, _ := d.table.Load().([]*Shard)
	tableSize := uint32(len(table))
	// d.rehashIndex must >= 0
	for {
		i := uint32(atomic.AddInt32(&d.rehashIndex, 1)) - 1
		if i >= tableSize {
			wg.Done()
			return
		}
		shard := d.getShard(i)
		shard.mutex.RLock()

		nextShard0 := d.nextTable[i]
		nextShard1 := d.nextTable[i+tableSize]

		nextShard0.mutex.RLock()
		nextShard1.mutex.RLock()

		var head0, head1 *Node
		var tail0, tail1 *Node
		node := shard.head
		for node != nil {
			// split current shard to 2 shards in next table
			if node.hashCode&tableSize == 0 {
				if head0 == nil {
					head0 = node
				} else {
					tail0.next = node
				}
				tail0 = node
			} else {
				if head1 == nil {
					head1 = node
				} else {
					tail1.next = node
				}
				tail1 = node
			}
			node = node.next
		}

		if tail0 != nil {
			tail0.next = nil
			nextShard0.head = head0
		}

		if tail1 != nil {
			tail1.next = nil
			nextShard1.head = head1
		}

		nextShard1.mutex.RUnlock()
		nextShard0.mutex.RUnlock()
		shard.mutex.RUnlock()
	}
}
