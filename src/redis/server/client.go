package server

import (
	"myGodis/src/lib/sync/atomic"
	"myGodis/src/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn net.Conn

	//waiting until reply finished
	waitingReply wait.Wait

	// is sending request in progress
	uploading atomic.AtomicBool
	// must bulk msg lineCount - 1 (first line)
	expectedArgsCount uint32
	// sent line count, exclude first line
	receivedCount uint32
	// sent lines, exclude first line
	args [][]byte

	// lock while server sending response
	mu sync.Mutex

	// subscribing channels
	subs map[string]bool
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

func (c *Client) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Write(b)
	return err
}

func (c *Client) SubsChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

func (c *Client) UnSubsChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs != nil {
		delete(c.subs, channel)
	}
}

func (c *Client) SubsCount() int {
	if c.subs == nil {
		return 0
	}
	return len(c.subs)
}

func (c *Client) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, 0, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}
