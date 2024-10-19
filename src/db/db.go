package db

import (
	"fmt"
	"myGodis/src/datastruct/dict"
	List "myGodis/src/datastruct/list"
	"myGodis/src/datastruct/lock"
	"myGodis/src/interface/redis"
	"myGodis/src/lib/logger"
	"myGodis/src/redis/reply"
	"runtime/debug"
	"strings"
	"time"
)

type DataEntity struct {
	Data interface{}
}

type DataEntityWithKey struct {
	DataEntity
	Key string
}

// args don't include cmd line
type CmdFunc func(db *DB, args [][]byte) redis.Reply

type DB struct {
	// key -> DataEntity
	Data *dict.Dict
	// key -> expireTime (time.Time)
	TTLMap *dict.Dict
	// channel -> list<*client>
	SubMap *dict.Dict

	// dict will ensure thread safety (by using mutex) of its method
	// use this mutex for complicated commands only, eg. rpush, incr ...
	Locker *lock.Locks

	// TimerTask interval
	interval time.Duration

	// channel -> list(*Client)
	subs *dict.Dict
	// lock channel
	subsLocker *lock.Locks
}

var router = MakeRouter()

func MakeDB() *DB {
	db := &DB{
		Data:     dict.Make(1024),
		TTLMap:   dict.Make(512),
		Locker:   lock.Make(128),
		interval: 5 * time.Second,

		subs:       dict.Make(16),
		subsLocker: lock.Make(16),
	}
	db.TimerTask()
	return db
}

func (db *DB) Exec(c redis.Client, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	if cmd == "subscribe" {
		if len(args) < 2 {
			return reply.MakeErrReply("ERR wrong number of arguments for 'subscribe' command")
		}
		return Subscribe(db, c, args[1:])
	} else if cmd == "unsubscribe" {
		return UnSubscribe(db, c, args[1:])
	}

	cmdFunc, ok := router[cmd]
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

func (db *DB) Get(key string) (*DataEntity, bool) {
	raw, ok := db.Data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

/* ---- Lock Function ---------------*/

func (db *DB) Lock(key string) {
	db.Locker.Lock(key)
}

func (db *DB) RLock(key string) {
	db.Locker.RLock(key)
}

func (db *DB) UnLock(key string) {
	db.Locker.UnLock(key)
}

func (db *DB) RUnlock(key string) {
	db.Locker.RUnlock(key)
}

func (db *DB) Locks(keys ...string) {
	db.Locker.Locks(keys...)
}

func (db *DB) RLocks(keys ...string) {
	db.Locker.RLocks(keys...)
}

func (db *DB) UnLocks(keys ...string) {
	db.Locker.UnLocks(keys...)
}

func (db *DB) RUnlocks(keys ...string) {
	db.Locker.RUnlocks(keys...)
}

/* ----- TTL Funtions -------*/
// 为key设置过期时间
func (db *DB) Expire(key string, expireTime time.Time) {
	db.TTLMap.Put(key, expireTime)
}

// 持久化保存
func (db *DB) Persist(key string) {
	db.TTLMap.Remove(key)
}

// 判断key是否过期
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.TTLMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

func (db *DB) Put(key string, entity *DataEntity) {
	db.Data.Put(key, entity)
}

func (db *DB) Remove(key string) {
	db.Data.Remove(key)
	db.TTLMap.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.Data.Get(key)
		if exists {
			db.Data.Remove(key)
			db.TTLMap.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) CleanExpired() {
	now := time.Now()
	toRemove := &List.LinkedList{}
	db.TTLMap.ForEach(func(key string, val interface{}) bool {
		expireTime, _ := val.(time.Time)
		if now.After(expireTime) {
			// expired
			toRemove.Add(key)
			db.Data.Remove(key)
		}
		return true
	})
	toRemove.ForEach(func(i int, val interface{}) bool {
		key, _ := val.(string)
		db.TTLMap.Remove(key)
		return true
	})
}

func (db *DB) TimerTask() {
	ticker := time.NewTicker(db.interval)
	go func() {
		for range ticker.C {
			db.CleanExpired()
		}
	}()
}

/* ----- Subscribe Functions ----- */
func (db *DB) AfterClientClose(c redis.Client) {
	unsubscribeAll(db, c)
}
