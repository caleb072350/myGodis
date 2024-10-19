package db

import (
	"myGodis/src/datastruct/dict"
	"myGodis/src/datastruct/list"
	"myGodis/src/datastruct/set"
	"myGodis/src/datastruct/sortedset"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
	"strconv"
	"time"
)

func Del(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	db.Locker.Locks(keys...)
	defer func() {
		db.Locker.Unlocks(keys...)
		db.Locker.Cleans(keys...)
	}()

	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

func Exists(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `exists` command")
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if exists {
		return reply.MakeIntReply(1)
	} else {
		return reply.MakeIntReply(0)
	}
}

func Type(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `type` command")
	}
	key := string(args[0])
	entity, exists := db.Get(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	case *list.LinkedList:
		return reply.MakeStatusReply("list")
	case *dict.Dict:
		return reply.MakeStatusReply("hash")
	case *set.Set:
		return reply.MakeStatusReply("set")
	case *sortedset.SortedSet:
		return reply.MakeStatusReply("zset")
	}
	return &reply.UnknownErrReply{}
}

func Rename(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `rename` command")
	}
	src := string(args[0])
	dest := string(args[1])

	db.Locks(src, dest)
	defer db.UnLocks(src, dest)

	entity, ok := db.Get(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.TTLMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.Put(dest, entity)
	if hasTTL {
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	return &reply.OkReply{}
}

func RenameNx(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `renamenx` command")
	}
	src := string(args[0])
	dest := string(args[1])

	db.Locks(src, dest)
	defer db.UnLocks(src, dest)

	_, ok := db.Get(dest)
	if ok {
		return reply.MakeIntReply(0)
	}

	entity, ok := db.Get(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.TTLMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.Put(dest, entity)
	if hasTTL {
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	return reply.MakeIntReply(1)
}

func Expire(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `expire` command")
	}
	key := string(args[0])
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR invalid expire time in `expire` command")
	}
	ttl := time.Duration(ttlArg) * time.Second

	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}
	db.Expire(key, time.Now().Add(ttl))
	return reply.MakeIntReply(1)
}

func ExpireAt(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `expireat` command")
	}
	key := string(args[0])
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR invalid expire time in `expireat` command")
	}
	ttl := time.Unix(ttlArg, 0)
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}
	db.Expire(key, ttl)
	return reply.MakeIntReply(1)
}

func PExpire(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `pexpire` command")
	}
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR invalid expire time in `pexpire` command")
	}
	ttl := time.Duration(ttlArg) * time.Millisecond

	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}
	db.Expire(key, time.Now().Add(ttl))
	return reply.MakeIntReply(1)
}

func PExpireAt(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for `pexpireat` command")
	}
	key := string(args[0])
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR invalid expire time in `pexpireat` command")
	}
	ttl := time.Unix(0, ttlArg*int64(time.Millisecond))
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}
	db.Expire(key, ttl)
	return reply.MakeIntReply(1)
}

func TTL(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `ttl` command")
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(-2)
	}
	raw, exists := db.TTLMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	// ttl := expireTime.Sub(time.Now())
	ttl := time.Until(expireTime)
	return reply.MakeIntReply(int64(ttl / time.Second))
}

func PTTL(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `pttl` command")
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(-2)
	}
	raw, exists := db.TTLMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := time.Until(expireTime)
	return reply.MakeIntReply(int64(ttl / time.Millisecond))
}

func Persist(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for `persist` command")
	}
	key := string(args[0])
	_, exists := db.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}
	db.TTLMap.Remove(key)
	return reply.MakeIntReply(1)
}
