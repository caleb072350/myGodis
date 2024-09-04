package server

import (
	"bufio"
	"context"
	"io"
	"myGodis/src/lib/logger"
	"myGodis/src/lib/sync/atomic"
	"myGodis/src/lib/sync/wait"
	"net"
	"sync"
)

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.AtomicBool
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

type Client struct {
	Conn    net.Conn
	Waiting wait.Wait
}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		conn.Close()
	}

	client := &Client{
		Conn: conn,
	}
	h.activeConn.Store(client, 1)

	reader := bufio.NewReader(conn)
	for {
		// may occurs: client EOF， client timeout， server early close
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(conn)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1)
		b := []byte(msg)
		conn.Write(b)
		client.Waiting.Done()
	}
}

func (h *EchoHandler) Close() error {
	logger.Info("handler shuting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key interface{}, value interface{}) bool {
		client := key.(*Client)
		client.Conn.Close()
		return true
	})
	return nil
}
