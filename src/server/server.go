package server

import (
	"context"
	"fmt"
	"myGodis/src/interface/tcp"
	"myGodis/src/lib/logger"
	"myGodis/src/lib/sync/atomic"
	"net"
	"os"
	"os/signal"
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
			listener.Close() // listener.Accept will return err immediately
		}
	}()

	// listen port
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	// closing listener than closing handler while shuting down
	defer handler.Close()
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				break
			}
			logger.Error(fmt.Sprintf("accept err: %v", err))
			continue
		}
		logger.Info(fmt.Sprintf("accept link from %s", conn.RemoteAddr().String()))
		go handler.Handle(ctx, conn)
	}
}
