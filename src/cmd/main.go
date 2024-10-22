package main

import (
	"fmt"
	"myGodis/src/config"
	"myGodis/src/lib/logger"
	RedisServer "myGodis/src/redis/server"
	"myGodis/src/tcp"
	"time"
)

func main() {
	config.SetupConfig("redis.conf")
	settings := &logger.Settings{
		Path:       "logs",
		Name:       "Godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	}
	logger.Setup(settings)

	cfg := &tcp.Config{
		Address:    fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		MaxConnect: uint32(config.Properties.MaxClients),
		Timeout:    2 * time.Second,
	}

	tcp.ListenAndServe(cfg, RedisServer.MakeHandler())
}
