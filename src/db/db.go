package db

import (
	"fmt"
	"myGodis/src/datastruct/dict"
	"myGodis/src/interface/redis"
	"myGodis/src/lib/logger"
	"myGodis/src/redis/reply"
	"runtime/debug"
	"strings"
	"sync"
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
	sync.RWMutex
}

// args don't include cmd line
type CmdFunc func(db *DB, args [][]byte) redis.Reply

type DB struct {
	Data *dict.Dict
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
		Data: dict.Make(1024),
	}
}
