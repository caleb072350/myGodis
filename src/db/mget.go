package db

import (
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
)

func MGet(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command ")
	}
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	result := make([][]byte, len(args))
	for i, key := range keys {
		val, exists := db.Data.Get(key)
		if !exists {
			result[i] = nil
			continue
		}
		entity, _ := val.(*DataEntity)
		if entity.Code != StringCode {
			result[i] = nil
			continue
		}
		bytes, _ := entity.Data.([]byte)
		result[i] = bytes
	}
	return reply.MakeMultiBulkReply(result)
}
