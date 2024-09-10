package main

import (
	"myGodis/src/lib/logger"
	"myGodis/src/redis/handler"
	"myGodis/src/server"
	"time"
)

func main() {
	settings := &logger.Settings{
		Path:       "logs",
		Name:       "Godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	}
	logger.Setup(settings)

	cfg := &server.Config{
		Address:    ":16379",
		MaxConnect: 16,
		Timeout:    2 * time.Second,
	}

	// handler := server.MakeEchoHandler()

	server.ListenAndServe(cfg, handler.MakeHandler())
}
