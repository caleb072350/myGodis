package db

import (
	SortedSet "myGodis/src/datastruct/sortedset"
	"myGodis/src/interface/redis"
	"myGodis/src/redis/reply"
	"strconv"
)

func (db *DB) getAsSortedSet(key string) (*SortedSet.SortedSet, reply.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*SortedSet.SortedSet)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string) (sortedSet *SortedSet.SortedSet, inited bool, errReply reply.ErrorReply) {
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = SortedSet.Make()
		db.Put(key, &DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

func ZAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || len(args)%2 != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zadd' command")
	}

	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*SortedSet.Element, size)
	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not a valid float")
		}
		elements[i] = &SortedSet.Element{
			Member: member,
			Score:  score,
		}
	}

	// lock
	db.Locker.Lock(key)
	defer db.Locker.UnLock(key)

	// get or init entity
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}
	i := 0
	for _, e := range elements {
		if sortedSet.Add(e.Member, e.Score) {
			i++
		}
	}
	return reply.MakeIntReply(int64(i))
}

func ZScore(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zscore' command")
	}

	key := string(args[0])
	member := string(args[1])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &reply.NullBulkReply{}
	}
	element, exists := sortedSet.Get(member)
	if !exists {
		return &reply.NullBulkReply{}
	}
	value := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return reply.MakeBulkReply([]byte(value))
}

func ZRank(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zrank' command")
	}

	key := string(args[0])
	member := string(args[1])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &reply.NullBulkReply{}
	}
	// get rank
	rank := sortedSet.GetRank(member, false)
	if rank < 0 {
		return &reply.NullBulkReply{}
	}
	return reply.MakeIntReply(rank)
}
