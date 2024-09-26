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

	rawEntity, exists := db.Data.Get(key)
	var entity *DataEntity
	if !exists {
		return &reply.NullBulkReply{}
	} else {
		entity, _ = rawEntity.(*DataEntity)
	}
	entity.RLock()
	defer entity.RUnlock()

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
