package db

import (
	"fmt"
	"myGodis/src/datastruct/dict"
	"myGodis/src/datastruct/lock"
	"myGodis/src/interface/redis"
	"myGodis/src/lib/logger"
	"myGodis/src/redis/reply"
	"runtime/debug"
	"strings"
)

const (
	StringCode = iota // Data is []byte
	ListCode          // *list.LinkedList
	SetCode
	DictCode // *dict.Dict
	SortedSetCode
)

type DataEntity struct {
	Code uint8
	TTL  int64 // ttl in seconds, 0 for unlimited ttl
	Data interface{}
}

type DataEntityWithKey struct {
	DataEntity
	Key string
}

// args don't include cmd line
type CmdFunc func(db *DB, args [][]byte) redis.Reply

type DB struct {
	Data *dict.Dict
	// dict will ensure thread safety (by using mutex) of its method
	// use this mutex for complicated commands only, eg. rpush, incr ...
	Locks *lock.LockMap
}

var cmdMap = MakeCmdMap()

func MakeCmdMap() map[string]CmdFunc {
	cmdMap := make(map[string]CmdFunc)
	cmdMap["ping"] = Ping
	cmdMap["get"] = Get
	cmdMap["set"] = Set
	cmdMap["setnx"] = SetNX
	cmdMap["setex"] = SetEX
	cmdMap["psetex"] = PSetEX
	cmdMap["mget"] = MGet
	cmdMap["mset"] = MSet

	cmdMap["getset"] = GetSet
	cmdMap["incr"] = Incr
	cmdMap["incrby"] = IncrBy
	cmdMap["incrbyfloat"] = IncrByFloat
	cmdMap["decr"] = Decr
	cmdMap["decrby"] = DecrBy

	cmdMap["lpush"] = LPush
	cmdMap["lpushx"] = LPushX
	cmdMap["rpush"] = RPush
	cmdMap["lpop"] = LPop
	cmdMap["rpop"] = RPop
	cmdMap["rpoplpush"] = RPopLPush
	cmdMap["lrem"] = LRem
	cmdMap["llen"] = LLen
	cmdMap["lindex"] = LIndex
	cmdMap["lset"] = LSet
	cmdMap["lrange"] = LRange

	return cmdMap
}

func (db *DB) Exec(args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	cmdFunc, ok := cmdMap[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		result = cmdFunc(db, args[1:])
	} else {
		result = cmdFunc(db, [][]byte{})
	}
	return
}

func MakeDB() *DB {
	return &DB{
		Data:  dict.Make(1024),
		Locks: &lock.LockMap{},
	}
}

func (db *DB) Get(key string) (*DataEntity, bool) {
	raw, ok := db.Data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

func (db *DB) Remove(key string) {
	db.Data.Remove(key)
	db.Locks.Clean(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.Locks.Locks(keys...)
	defer func() {
		db.Locks.Unlocks(keys...)
		db.Locks.Cleans(keys...)
	}()
	deleted = 0
	for _, key := range keys {
		_, exists := db.Data.Get(key)
		if exists {
			db.Data.Remove(key)
			deleted++
		}
	}
	return deleted
}
