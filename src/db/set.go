package db

import (
	HashSet "myGodis/src/datastruct/set"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
)

func (db *DB) getAsSet(key string) (*HashSet.Set, reply.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return set, nil
}

func (db *DB) GetOrInitSet(key string) (set *HashSet.Set, inited bool, errReply reply.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = HashSet.Make()
		db.Put(key, &DataEntity{
			Data: set,
		})
		inited = true
	}
	return set, inited, nil
}

func SAdd(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'sadd' command"), nil
	}
	key := string(args[0])
	members := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	// get or init entity
	set, _, errReply := db.GetOrInitSet(key)
	if errReply != nil {
		return errReply, nil
	}

	counter := 0
	for _, member := range members {
		counter += set.Add(string(member))
	}
	return reply.MakeIntReply(int64(counter)), &extra{toPersist: true}
}

func SIsMember(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sismember` command"), nil
	}
	key := string(args[0])
	member := string(args[1])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply, nil
	}
	if set == nil {
		return reply.MakeIntReply(0), nil
	}

	has := set.Has(member)
	if has {
		return reply.MakeIntReply(1), nil
	} else {
		return reply.MakeIntReply(0), nil
	}
}

func SRem(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'srem' command"), nil
	}
	key := string(args[0])
	members := args[1:]

	// lock
	db.Lock(key)
	defer db.UnLock(key)

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply, nil
	}
	if set == nil {
		return reply.MakeIntReply(0), nil
	}

	counter := 0
	for _, member := range members {
		counter += set.Remove(string(member))
	}
	if set.Len() == 0 {
		db.Remove(key)
	}
	return reply.MakeIntReply(int64(counter)), &extra{toPersist: counter > 0}
}

func SCard(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `scard` command"), nil
	}
	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply, nil
	}
	if set == nil {
		return reply.MakeIntReply(0), nil
	}
	return reply.MakeIntReply(int64(set.Len())), nil
}

func SMembers(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `smembers` command"), nil
	}
	key := string(args[0])

	// lock
	db.Locker.Lock(key)
	defer db.Locker.UnLock(key)

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply, nil
	}
	if set == nil {
		return &reply.EmptyMultiBulkReply{}, nil
	}
	arr := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr), nil
}

func SInter(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sinter` command"), nil
	}

	keys := make([]string, len(args))

	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.Locker.RLocks(keys...)
	defer db.Locker.RUnlocks(keys...)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			return &reply.EmptyMultiBulkReply{}, nil
		}
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				return &reply.EmptyMultiBulkReply{}, nil
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
	return reply.MakeMultiBulkReply(arr), nil
}

func SInterStore(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sinterstore` command"), nil
	}
	dest := string(args[0])
	keys := make([]string, len(args)-1)

	for i, arg := range args[1:] {
		keys[i] = string(arg)
	}

	// lock
	db.Locker.RLocks(keys...)
	defer db.Locker.RUnlocks(keys...)
	db.Locker.Lock(dest)
	defer db.Locker.UnLock(dest)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			db.Remove(dest) // clean ttl and old value
			return &reply.EmptyMultiBulkReply{}, nil
		}
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest) // clean ttl and old value
				return reply.MakeIntReply(0), nil
			}
		}
	}

	set := HashSet.MakeFromVals(result.ToSlice()...)
	entity := &DataEntity{
		Data: set,
	}
	db.Put(dest, entity)

	return reply.MakeIntReply(int64(set.Len())), &extra{toPersist: true}
}

func SUnion(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sunion` command"), nil
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.Locker.RLocks(keys...)
	defer db.Locker.RUnlocks(keys...)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			continue
		}
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	if result == nil {
		return &reply.EmptyMultiBulkReply{}, nil
	}
	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr), nil
}

func SUnionStore(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sunionstore` command"), nil
	}
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		keys[i] = string(arg)
	}

	// lock
	db.Locker.RLocks(keys...)
	defer db.Locker.RUnlocks(keys...)
	db.Locker.Lock(dest)
	defer db.Locker.UnLock(dest)

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			continue
		}
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	db.Remove(dest) // clean ttl
	if result == nil {
		return &reply.EmptyMultiBulkReply{}, nil
	}

	set := HashSet.MakeFromVals(result.ToSlice()...)
	entity := &DataEntity{
		Data: set,
	}
	db.Put(dest, entity)
	return reply.MakeIntReply(int64(set.Len())), &extra{toPersist: true}
}

func SDiff(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sdiff` command"), nil
	}

	keys := make([]string, len(args))

	for i, arg := range args {
		keys[i] = string(arg)
	}

	// lock
	db.Locker.RLocks(keys...)
	defer db.Locker.RUnlocks(keys...)

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			if i == 0 {
				return &reply.EmptyMultiBulkReply{}, nil
			}
			continue
		}
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				return &reply.EmptyMultiBulkReply{}, nil
			}
		}
	}

	if result == nil {
		// all keys are nil
		return &reply.EmptyMultiBulkReply{}, nil
	}

	arr := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(arr), nil
}

func SDiffStore(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `sdiffstore` command"), nil
	}
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		keys[i] = string(arg)
	}

	// lock
	db.Locker.RLocks(keys...)
	defer db.Locker.RUnlocks(keys...)
	db.Locker.Lock(dest)
	defer db.Locker.UnLock(dest)

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			if i == 0 {
				db.Remove(dest)
				return &reply.EmptyMultiBulkReply{}, nil
			} else {
				continue
			}
		}
		if result == nil {
			result = HashSet.MakeFromVals(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest)
				return &reply.EmptyMultiBulkReply{}, nil
			}
		}
	}

	if result == nil {
		// all key are nil
		db.Remove(dest)
		return &reply.EmptyMultiBulkReply{}, nil
	}
	set := HashSet.MakeFromVals(result.ToSlice()...)
	entity := &DataEntity{
		Data: set,
	}
	db.Put(dest, entity)

	return reply.MakeIntReply(int64(set.Len())), &extra{toPersist: true}
}
