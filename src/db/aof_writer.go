package db

import (
	"myGodis/src/lib/logger"
	"myGodis/src/redis/reply"
	"strconv"
	"time"
)

var pExpiredAtCmd = []byte("PEXPIREAT")

func makeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpiredAtCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}

// send command to aof
func (db *DB) addAof(args *reply.MultiBulkReply) {
	db.aofChan <- args
}

// listen aof file
func (db *DB) handleAof() {
	for cmd := range db.aofChan {
		_, err := db.aofFile.Write(cmd.ToBytes())
		if err != nil {
			logger.Warn(err)
		}
	}
}
