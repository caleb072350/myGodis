package db

import (
	List "myGodis/src/datastruct/list"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
)

func RPush(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// get or init entity
	rawEntity, exists := db.Data.Get(key)
	var entity *DataEntity
	if !exists {
		entity = &DataEntity{
			Code: ListCode,
			Data: &List.LinkedList{},
		}
	} else {
		entity, _ = rawEntity.(*DataEntity)
	}
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}
	entity.Lock()
	defer entity.Unlock()

	// put list
	list, _ := entity.Data.(*List.LinkedList)
	for _, value := range values {
		list.Add(value)
	}
	db.Data.Put(key, entity)
	return reply.MakeIntReply(int64(list.Len()))
}
