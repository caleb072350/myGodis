package db

import (
	HashSet "myGodis/src/datastruct/set"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
)

func SAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'sadd' command")
	}
	key := string(args[0])
	members := args[1:]

	// lock
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	// get or init entity
	entity, exists := db.Get(key)
	if !exists {
		entity = &DataEntity{
			Code: SetCode,
			Data: HashSet.Make(0),
		}
		db.Data.Put(key, entity)
	}

	// check type
	if entity.Code != SetCode {
		return &reply.WrongTypeErrReply{}
	}

	set, _ := entity.Data.(*HashSet.Set)
	counter := 0
	for _, member := range members {
		counter += set.Add(string(member))
	}
	return reply.MakeIntReply(int64(counter))
}

func SIsMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sismember` command")
	}
	key := string(args[0])
	member := string(args[1])

	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	// check type
	if entity.Code != SetCode {
		return &reply.WrongTypeErrReply{}
	}

	set, _ := entity.Data.(*HashSet.Set)
	has := set.Has(member)
	if has {
		return reply.MakeIntReply(1)
	} else {
		return reply.MakeIntReply(0)
	}
}

func SRem(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'srem' command")
	}
	key := string(args[0])
	members := args[1:]

	// lock
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	// check type
	if entity.Code != SetCode {
		return &reply.WrongTypeErrReply{}
	}

	set, _ := entity.Data.(*HashSet.Set)

	counter := 0
	for _, member := range members {
		counter += set.Remove(string(member))
	}
	return reply.MakeIntReply(int64(counter))
}

func SCard(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `scard` command")
	}
	key := string(args[0])

	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	// check type
	if entity.Code != SetCode {
		return &reply.WrongTypeErrReply{}
	}

	set, _ := entity.Data.(*HashSet.Set)
	return reply.MakeIntReply(int64(set.Len()))
}

func SMembers(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `smembers` command")
	}
	key := string(args[0])

	// lock
	db.Locks.Lock(key)
	defer db.Locks.Unlock(key)

	entity, exists := db.Get(key)
	if !exists {
		return &reply.EmptyMultiBulkReply{}
	}

	// check type
	if entity.Code != SetCode {
		return &reply.WrongTypeErrReply{}
	}

	set, _ := entity.Data.(*HashSet.Set)
	arr := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

func SInter(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sinter` command")
	}

	keys := make([]string, len(args))

	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.Locks.RLocks(keys...)
	defer db.Locks.RUnlocks(keys...)

	var result *HashSet.Set
	for _, key := range keys {
		entity, exists := db.Get(key)
		if !exists {
			return &reply.EmptyMultiBulkReply{}
		}
		if entity.Code != SetCode {
			return &reply.WrongTypeErrReply{}
		}
		set, _ := entity.Data.(*HashSet.Set)
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				return &reply.EmptyMultiBulkReply{}
			}
		}
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

func SInterStore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sinterstore` command")
	}
	dest := string(args[0])
	keys := make([]string, len(args)-1)

	for i, arg := range args[1:] {
		keys[i] = string(arg)
	}

	// lock
	db.Locks.RLocks(keys...)
	defer db.Locks.RUnlocks(keys...)
	db.Locks.Lock(dest)
	defer db.Locks.Unlock(dest)

	var result *HashSet.Set
	for _, key := range keys {
		entity, exists := db.Get(key)
		if !exists {
			db.Remove(dest) // clean ttl and old value
			return reply.MakeIntReply(0)
		}
		if entity.Code != SetCode {
			return &reply.WrongTypeErrReply{}
		}
		set, _ := entity.Data.(*HashSet.Set)
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest) // clean ttl and old value
				return reply.MakeIntReply(0)
			}
		}
	}

	set := HashSet.MakeFromVals(result.ToSlice()...)
	entity := &DataEntity{
		Code: SetCode,
		Data: set,
	}
	db.Data.Put(dest, entity)

	return reply.MakeIntReply(int64(set.Len()))
}

func SUnion(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sunion` command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.Locks.RLocks(keys...)
	defer db.Locks.RUnlocks(keys...)

	var result *HashSet.Set
	for _, key := range keys {
		entity, exists := db.Get(key)
		if !exists {
			continue
		}
		if entity.Code != SetCode {
			return &reply.WrongTypeErrReply{}
		}
		set, _ := entity.Data.(*HashSet.Set)
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	if result == nil {
		return &reply.EmptyMultiBulkReply{}
	}
	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

func SUnionStore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sunionstore` command")
	}
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		keys[i] = string(arg)
	}

	// lock
	db.Locks.RLocks(keys...)
	defer db.Locks.RUnlocks(keys...)
	db.Locks.Lock(dest)
	defer db.Locks.Unlock(dest)

	var result *HashSet.Set
	for _, key := range keys {
		entity, exists := db.Get(key)
		if !exists {
			continue
		}
		if entity.Code != SetCode {
			return &reply.WrongTypeErrReply{}
		}
		set, _ := entity.Data.(*HashSet.Set)
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	db.Remove(dest) // clean ttl
	if result == nil {
		return &reply.EmptyMultiBulkReply{}
	}

	set := HashSet.MakeFromVals(result.ToSlice()...)
	entity := &DataEntity{
		Code: SetCode,
		Data: set,
	}
	db.Data.Put(dest, entity)
	return reply.MakeIntReply(int64(set.Len()))
}

func SDiff(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sdiff` command")
	}

	keys := make([]string, len(args))

	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.Locks.RLocks(keys...)
	defer db.Locks.RUnlocks(keys...)

	var result *HashSet.Set
	for i, key := range keys {
		entity, exists := db.Get(key)
		if !exists {
			if i == 0 {
				// early termination
				return &reply.EmptyMultiBulkReply{}
			}
			continue
		}
		if entity.Code != SetCode {
			return &reply.WrongTypeErrReply{}
		}
		set, _ := entity.Data.(*HashSet.Set)
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				return &reply.EmptyMultiBulkReply{}
			}
		}
	}

	if result == nil {
		// all keys are nil
		return &reply.EmptyMultiBulkReply{}
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr)
}

func SDiffStore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sdiffstore` command")
	}
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		keys[i] = string(arg)
	}

	// lock
	db.Locks.RLocks(keys...)
	defer db.Locks.RUnlocks(keys...)
	db.Locks.Lock(dest)
	defer db.Locks.Unlock(dest)

	var result *HashSet.Set
	for i, key := range keys {
		entity, exists := db.Get(key)
		if !exists {
			if i == 0 {
				// early termination
				db.Remove(dest)
				return &reply.EmptyMultiBulkReply{}
			}
			continue
		}
		if entity.Code != SetCode {
			return &reply.WrongTypeErrReply{}
		}
		set, _ := entity.Data.(*HashSet.Set)
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest)
				return &reply.EmptyMultiBulkReply{}
			}
		}
	}

	if result == nil {
		// all key are nil
		db.Remove(dest)
		return &reply.EmptyMultiBulkReply{}
	}
	set := HashSet.MakeFromVals(result.ToSlice()...)
	entity := &DataEntity{
		Code: SetCode,
		Data: set,
	}
	db.Data.Put(dest, entity)

	return reply.MakeIntReply(int64(set.Len()))
}
