package db

import (
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
	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
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
