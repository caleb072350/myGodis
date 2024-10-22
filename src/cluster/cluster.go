package cluster

import (
	"myGodis/src/interface/db"
	"myGodis/src/lib/consistenthash"
)

type Cluster struct {
	self       string
	db         *db.DB
	peerPicker *consistenthash.Map
	peers      map[string]*client.Client
}
