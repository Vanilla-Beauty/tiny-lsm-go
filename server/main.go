package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/logger"
)

func main() {
	var (
		address = flag.String("addr", ":6379", "Redis server address")
		dbPath  = flag.String("db-path", "redis_data", "Path to database files")
	)

	flag.Parse()

	// Load configuration
	cfg := config.DefaultConfig()
	// You can load from file if needed: config.LoadFromFile("config.toml", cfg)

	// Initialize logger
	if err := logger.InitLoggerFile(cfg.Logger.LogDir, cfg.Logger.EnableFileLogging); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	// Create the server
	server, err := NewRedisServer(*address, *dbPath)
	if err != nil {
		logger.Fatalf("Failed to create Redis server: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		logger.Fatalf("Failed to start Redis server: %v", err)
	}

	// Wait for interrupt signal to gracefully shutdown the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down Redis server...")

	// Stop the server
	if err := server.Stop(); err != nil {
		logger.Fatalf("Error stopping server: %v", err)
	}

	logger.Info("Server stopped")
}
