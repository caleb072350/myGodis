package db

import (
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
)

func Get(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'get' command")
	}
	key := string(args[0])
	val, ok := db.Data.Get(key)
	// logger.Info("GET: key: " + key + " value: " + string(val.(*DataEntity).Data.([]byte)))
	if !ok {
		return &reply.NullBulkReply{}
	}
	entity, _ := val.(*DataEntity)
	if entity.Code == StringCode {
		bytes, ok := entity.Data.([]byte)
		if !ok {
			return &reply.UnknownErrReply{}
		}
		return reply.MakeBulkReply(bytes)
	} else {
		return reply.MakeErrReply("ERR get only support string")
	}
}
