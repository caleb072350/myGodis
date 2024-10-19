package handler

import (
	"myGodis/src/lib/sync/atomic"
	"myGodis/src/lib/sync/wait"
	"net"
	"time"
)

type Client struct {
	conn net.Conn

	//waiting until reply finished
	waitingReply wait.Wait

	// is sending request in progress
	sending atomic.AtomicBool
	// must bulk msg lineCount - 1 (first line)
	expectedArgsCount uint32
	// sent line count, exclude first line
	receivedCount uint32
	// sent lines, exclude first line
	args [][]byte
}

func (c *Client) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func MakeClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}
