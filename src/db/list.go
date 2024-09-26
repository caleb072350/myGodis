package db

import (
	List "myGodis/src/datastruct/list"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
	"strconv"
)

func LIndex(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of args for 'lindex' command")
	}
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	entity, exists := db.Get(key)
	if !exists {
		return &reply.NullBulkReply{}
	}

	//check type
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	size := list.Len()
	if index < -1*size || index >= size {
		return &reply.NullBulkReply{}
	} else if index < 0 {
		index = size + index
	}
	//get value
	val := list.Get(index).([]byte)
	return reply.MakeBulkReply(val)

}

func LLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'llen' command")
	}
	key := string(args[0])

	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	//check type
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	size := int64(list.Len())
	return reply.MakeIntReply(size)
}

func LPop(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'lpop' command")
	}

	key := string(args[0])

	// lock key
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get data
	entity, exists := db.Get(key)
	if !exists {
		return &reply.NullBulkReply{}
	}

	// check type
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.Data.Remove(key)
	}
	return reply.MakeBulkReply(val)
}

func LPush(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR number of args for'lpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock key
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get or init entity
	entity, exists := db.Get(key)
	if !exists {
		entity = &DataEntity{
			Code: ListCode,
			Data: &List.LinkedList{},
		}
	}

	if entity.Code != ListCode {
		return reply.MakeErrReply("ERR type error")
	}

	// insert
	list, _ := entity.Data.(*List.LinkedList)
	for _, value := range values {
		list.Insert(0, value)
	}

	db.Data.Put(key, entity)

	return reply.MakeIntReply(int64(list.Len()))
}

// 只有在key存在的情况下才执行插入，否则不执行插入操作
func LPushX(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR number of args for'lpushx' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock key
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	if entity.Code != ListCode {
		return reply.MakeErrReply("ERR type error")
	}

	list, _ := entity.Data.(*List.LinkedList)
	for _, value := range values {
		list.Insert(0, value)
	}

	db.Data.Put(key, entity)

	return reply.MakeIntReply(int64(list.Len()))
}

func LRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'lrange' command")
	}
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)

	// get data
	entity, exists := db.Get(key)
	if !exists {
		return &reply.EmptyMultiBulkReply{}
	}
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	// compute index
	list, _ := entity.Data.(*List.LinkedList)
	size := list.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &reply.EmptyMultiBulkReply{}
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop += 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size-1], stop in [start, size]
	slice := list.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}
	return reply.MakeMultiBulkReply(result)
}

func LRem(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 {
		return reply.MakeErrReply(("ERR wrong number of arguments for 'lrem' command"))
	}
	key := string(args[0])
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	// lock key
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get data entity
	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(value)
	} else if count > 0 {
		removed = list.RemoveByVal(value, count)
	} else {
		removed = list.ReverseRemoveByVal(value, -count)
	}

	if list.Len() == 0 {
		db.Data.Remove(key)
	}

	return reply.MakeIntReply(int64(removed))
}

func LSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'lset' command")
	}
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	// lock key
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get entity
	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeErrReply("ERR no such key")
	}
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	size := list.Len()
	if index < -1*size {
		return reply.MakeErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.MakeErrReply("ERR index out of range")
	}

	list.Set(index, value)
	return &reply.OkReply{}
}

func RPop(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpop' command")
	}
	key := string(args[0])

	// lock key
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get entity
	entity, exists := db.Get(key)
	if !exists {
		return &reply.NullBulkReply{}
	}
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Data.Remove(key)
	}
	return reply.MakeBulkReply(val)
}

func RPopLPush(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpoplpush' command")
	}
	sourceKey := string(args[0])
	destKey := string(args[1])

	// locks keys
	db.Locks.Locks(sourceKey, destKey)
	defer db.Locks.Unlocks(sourceKey, destKey)

	// get source entity
	sourceEntity, exists := db.Get(sourceKey)
	if !exists {
		return &reply.NullBulkReply{}
	}
	sourceList, _ := sourceEntity.Data.(*List.LinkedList)

	// get dest entity
	destEntity, exists := db.Get(destKey)
	if !exists {
		destEntity = &DataEntity{
			Code: ListCode,
			Data: &List.LinkedList{},
		}
		db.Data.Put(destKey, destEntity)
	}
	destList, _ := destEntity.Data.(*List.LinkedList)

	// pop and push
	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.Remove(sourceKey)
	}
	return reply.MakeBulkReply(val)
}

func RPush(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get or init entity
	entity, exists := db.Get(key)
	if !exists {
		entity = &DataEntity{
			Code: ListCode,
			Data: &List.LinkedList{},
		}
	}
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	// put list
	list, _ := entity.Data.(*List.LinkedList)
	for _, value := range values {
		list.Add(value)
	}
	db.Data.Put(key, entity)
	return reply.MakeIntReply(int64(list.Len()))
}
