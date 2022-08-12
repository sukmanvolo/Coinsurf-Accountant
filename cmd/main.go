package main

import (
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/coinsurf-com/accountant/pkg/server"
	"github.com/jessevdk/go-flags"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
)

func main() {
	var config server.Config

	parser := flags.NewParser(&config, flags.Default)
	if _, err := parser.Parse(); err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt)

	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors:      true,
		DisableTimestamp: false,
		FullTimestamp:    true,
	})

	logger.SetLevel(logrus.InfoLevel)
	if config.Debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	logger.Info("Starting...")
	defer logger.Info("Stopping...")

	err := server.Listen(signals, &config, logger)
	if err != nil {
		logger.Error("failed to listen: " + err.Error())
	}

	close(signals)
}
