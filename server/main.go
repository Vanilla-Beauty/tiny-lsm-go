package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		address = flag.String("addr", ":6379", "Redis server address")
		dbPath  = flag.String("db-path", "redis_data", "Path to database files")
	)

	flag.Parse()

	// Create the server
	server, err := NewRedisServer(*address, *dbPath)
	if err != nil {
		log.Fatalf("Failed to create Redis server: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start Redis server: %v", err)
	}

	// Wait for interrupt signal to gracefully shutdown the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down Redis server...")

	// Stop the server
	if err := server.Stop(); err != nil {
		log.Fatalf("Error stopping server: %v", err)
	}

	log.Println("Server stopped")
}
