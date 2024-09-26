package db

import (
	List "myGodis/src/datastruct/list"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
)

func LPush(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR number of args for'lpush' command")
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

	entity.Lock()
	defer entity.Unlock()

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

	rawEntity, exists := db.Data.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	entity, _ := rawEntity.(*DataEntity)
	entity.Lock()
	defer entity.Unlock()

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
