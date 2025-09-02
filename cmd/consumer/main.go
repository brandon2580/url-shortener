package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	"url-shortener/internal/events"
	"url-shortener/internal/store"

	_ "github.com/lib/pq" // PostgreSQL driver
)

func main() {
	// Get configuration from environment variables
	dbURL := getEnvOrDefault("DATABASE_URL", "postgres://user:password@localhost:5432/urlshortener?sslmode=disable")
	rabbitURL := getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	log.Printf("Starting click events consumer service...")

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Test database connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}
	log.Printf("Connected to PostgreSQL successfully")

	// Create click store
	clickStore := store.NewClickStore(db)

	// Create and start batch consumer
	consumer, err := events.NewBatchConsumer(rabbitURL, clickStore)
	if err != nil {
		log.Fatalf("Failed to create batch consumer: %v", err)
	}
	defer consumer.Stop()

	// Start consuming messages
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Consumer service started successfully. Press Ctrl+C to stop...")
	log.Printf("Batch processing: 50 events per batch, 5-second timeout")

	// Wait for termination signal
	<-sigChan

	log.Printf("Received termination signal, shutting down gracefully...")
	consumer.Stop()
	log.Printf("Consumer service stopped")
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
