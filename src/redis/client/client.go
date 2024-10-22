package client

import (
	"context"
	"myGodis/src/interface/redis"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn        net.Conn
	sendingReqs chan *Request
	waitingReqs chan *Request
	ticker      *time.Ticker
	addr        string

	ctx        context.Context
	cancelFunc context.CancelFunc
	writing    *sync.WaitGroup
}

type Request struct {
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *sync.WaitGroup
}

const (
	chanSize = 256
)

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		conn:        conn,
		addr:        addr,
		sendingReqs: make(chan *Request, chanSize),
		waitingReqs: make(chan *Request, chanSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		writing:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) Start() {

}
