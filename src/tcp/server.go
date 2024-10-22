package tcp

import (
	"context"
	"fmt"
	"myGodis/src/interface/tcp"
	"myGodis/src/lib/logger"
	"myGodis/src/lib/sync/atomic"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServe(cfg *Config, handler tcp.Handler) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		logger.Fatal(fmt.Sprintf("listen err: %v", err))
	}

	// listen signal
	var closing atomic.AtomicBool
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logger.Info("shuting down...")
			closing.Set(true)
			_ = listener.Close() // listener.Accept will return err immediately
			_ = handler.Close()  // close connections
		}
	}()

	// listen port
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	// closing listener than closing handler while shuting down
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				logger.Info("waitting disconnect...")
				waitDone.Wait()
				return // handler will be closed by defer
			}
			logger.Error(fmt.Sprintf("accept err: %v", err))
			continue
		}
		logger.Info(fmt.Sprintf("accept link from %s", conn.RemoteAddr().String()))
		go func() {
			defer func() {
				waitDone.Done()
			}()
			waitDone.Add(1)
			handler.Handle(ctx, conn)
		}()
	}
}
