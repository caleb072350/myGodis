package server

import (
	"bufio"
	"context"
	"io"
	DBImpl "myGodis/src/db"
	"myGodis/src/interface/db"
	"myGodis/src/lib/logger"
	"myGodis/src/lib/sync/atomic"
	"myGodis/src/redis/reply"
	"net"
	"strconv"
	"strings"
	"sync"
)

/*
* A tcp.Handler implements redis protocol
 */

var (
	UnknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         db.DB
	closing    atomic.AtomicBool // refusing new client and new request
}

func MakeHandler() *Handler {
	return &Handler{
		db: DBImpl.MakeDB(),
	}
}

func (h *Handler) closeClient(client *Client) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}
	client := MakeClient(conn)
	h.activeConn.Store(client, 1)

	reader := bufio.NewReader(conn)
	var fixedLen int64 = 0
	var err error
	var msg []byte
	for {
		// may occurs: client EOF, client timeout, server early close
		if fixedLen == 0 {
			msg, err = reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF ||
					strings.Contains(err.Error(), "use of closed network connection") {
					logger.Info("connection close")
				} else {
					logger.Warn(err)
				}
				// after client close
				h.closeClient(client)
				return // io error, disconnect with client
			}
			if len(msg) == 0 || msg[len(msg)-2] != '\r' {
				errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
				_, _ = client.conn.Write(errReply.ToBytes())
			}
		} else {
			msg = make([]byte, fixedLen+2)
			_, err = io.ReadFull(reader, msg)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF ||
					strings.Contains(err.Error(), "use of closed network connection") {
					logger.Info("connection close")
				} else {
					logger.Warn(err)
				}
				// after client close
				h.closeClient(client)
				return // io error, disconnect with client
			}
			if len(msg) == 0 ||
				msg[len(msg)-2] != '\r' ||
				msg[len(msg)-1] != '\n' {
				errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
				_, _ = client.conn.Write(errReply.ToBytes())
			}

			fixedLen = 0
		}

		if !client.uploading.Get() {
			// new request
			if msg[0] == '*' {
				// bulk multi msg
				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					_, _ = client.conn.Write(UnknownErrReplyBytes)
					continue
				}
				client.waitingReply.Add(1)
				client.uploading.Set(true)
				client.expectedArgsCount = uint32(expectedLine)
				client.receivedCount = 0
				client.args = make([][]byte, expectedLine)
			} else {
				// TODO: text protocol
				// remove \r or \n or \r\n in the end of line
				str := strings.TrimSuffix(string(msg), "\n")
				str = strings.TrimSuffix(str, "\r")
				strs := strings.Split(str, " ")
				args := make([][]byte, len(strs))
				for i, s := range strs {
					args[i] = []byte(s)
				}

				// send reply
				result := h.db.Exec(client, args)
				if result != nil {
					_ = client.Write(result.ToBytes())
				} else {
					_ = client.Write(UnknownErrReplyBytes)
				}
			}
		} else {
			// receive following part of a request
			line := msg[0 : len(msg)-2]
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
				if err != nil {
					errReply := &reply.ProtocolErrReply{Msg: err.Error()}
					_, _ = client.conn.Write(errReply.ToBytes())
				}
				if fixedLen <= 0 {
					errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
					_, _ = client.conn.Write(errReply.ToBytes())
				}
			} else {
				client.args[client.receivedCount] = line
				client.receivedCount++
			}

			// if sending finished
			if client.receivedCount == client.expectedArgsCount {
				client.uploading.Set(false) // finish sending progress

				// send reply
				result := h.db.Exec(client, client.args)
				if result != nil {
					_ = client.Write(result.ToBytes())
				} else {
					_ = client.Write(UnknownErrReplyBytes)
				}

				// finish reply
				client.expectedArgsCount = 0
				client.receivedCount = 0
				client.args = nil
				client.waitingReply.Done()
			}
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shuting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value any) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	h.db.Close()
	return nil
}
