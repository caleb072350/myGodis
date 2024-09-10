package db

import "myGodis/src/interface/redis"

type DB interface {
	Exec([][]byte) redis.Reply
}
