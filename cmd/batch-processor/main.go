package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"url-shortener/internal/cache"
	"url-shortener/internal/models"
	"url-shortener/internal/store"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/streadway/amqp"
)

// BatchProcessor handles daily batch processing of click events and Redis stats aggregation
type BatchProcessor struct {
	rabbitConn *amqp.Connection
	channel    *amqp.Channel
	store      *store.ClickStore
	redisCache *cache.Redis
	queueName  string
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(rabbitURL, dbURL, redisAddr string) (*BatchProcessor, error) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	// Connect to Redis
	redisCache, err := cache.Connect(redisAddr)
	if err != nil {
		db.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Create click store
	clickStore := store.NewClickStore(db)

	return &BatchProcessor{
		rabbitConn: conn,
		channel:    channel,
		store:      clickStore,
		redisCache: redisCache,
		queueName:  "click_events",
	}, nil
}

// ProcessDailyBatch processes all queued click events in one go
func (bp *BatchProcessor) ProcessDailyBatch() error {
	log.Printf("🌙 Starting daily batch processing...")
	startTime := time.Now()

	// Get queue info to see how many messages we have
	queue, err := bp.channel.QueueInspect(bp.queueName)
	if err != nil {
		return fmt.Errorf("failed to inspect queue: %w", err)
	}

	totalMessages := queue.Messages
	log.Printf("📊 Found %d messages in queue to process", totalMessages)

	if totalMessages == 0 {
		log.Printf("✅ No messages to process, exiting")
		return nil
	}

	// Process messages in large batches for efficiency
	batchSize := 1000 // Much larger batches for nightly processing
	processedCount := 0
	failedCount := 0

	for processedCount < totalMessages {
		batch, err := bp.consumeBatch(batchSize)
		if err != nil {
			log.Printf("❌ Error consuming batch: %v", err)
			break
		}

		if len(batch) == 0 {
			log.Printf("🔚 No more messages in queue")
			break
		}

		// Extract events from batch
		events := make([]models.ClickEvent, 0, len(batch))
		deliveries := make([]amqp.Delivery, 0, len(batch))

		for _, item := range batch {
			var event models.ClickEvent
			if err := json.Unmarshal(item.delivery.Body, &event); err != nil {
				log.Printf("⚠️ Warning: Failed to unmarshal event, skipping: %v", err)
				item.delivery.Ack(false) // Remove bad message
				failedCount++
				continue
			}
			events = append(events, event)
			deliveries = append(deliveries, item.delivery)
		}

		if len(events) == 0 {
			continue
		}

		// Store batch in PostgreSQL
		err = bp.store.RecordClickBatch(events)
		if err != nil {
			log.Printf("❌ Error storing batch: %v", err)
			// Nack messages for retry
			for _, delivery := range deliveries {
				delivery.Nack(false, true)
			}
			failedCount += len(events)
		} else {
			// Acknowledge successful processing
			for _, delivery := range deliveries {
				delivery.Ack(false)
			}
			processedCount += len(events)
			log.Printf("✅ Processed batch: %d events (Total: %d/%d)", len(events), processedCount, totalMessages)
		}
	}

	duration := time.Since(startTime)
	log.Printf("🎉 Daily batch processing completed!")
	log.Printf("📈 Statistics:")
	log.Printf("   • Total processed: %d events", processedCount)
	log.Printf("   • Failed: %d events", failedCount)
	log.Printf("   • Duration: %v", duration)
	log.Printf("   • Rate: %.2f events/second", float64(processedCount)/duration.Seconds())

	return nil
}

// BatchItem holds a delivery for batch processing
type BatchItem struct {
	delivery amqp.Delivery
}

// consumeBatch consumes up to batchSize messages from the queue
func (bp *BatchProcessor) consumeBatch(batchSize int) ([]BatchItem, error) {
	var batch []BatchItem

	for i := 0; i < batchSize; i++ {
		delivery, ok, err := bp.channel.Get(bp.queueName, false) // manual ack
		if err != nil {
			return nil, fmt.Errorf("failed to get message: %w", err)
		}
		if !ok {
			// No more messages
			break
		}

		batch = append(batch, BatchItem{delivery: delivery})
	}

	return batch, nil
}

// ProcessRedisDailyStats processes yesterday's Redis daily stats and stores them in PostgreSQL
func (bp *BatchProcessor) ProcessRedisDailyStats() error {
	log.Printf("📊 Starting Redis daily stats aggregation...")
	startTime := time.Now()

	// Process yesterday's data (assuming the job runs at night)
	yesterday := time.Now().UTC().AddDate(0, 0, -1)

	// Get all daily stats keys from Redis with pattern stats:*:YYYY-MM-DD
	pattern := fmt.Sprintf("stats:*:%s", yesterday.UTC().Format("2006-01-02"))

	keys, err := bp.redisCache.GetKeysByPattern(pattern)
	if err != nil {
		return fmt.Errorf("failed to get Redis stats keys: %w", err)
	}

	log.Printf("📈 Found %d daily stats keys for %s", len(keys), yesterday.UTC().Format("2006-01-02"))

	if len(keys) == 0 {
		log.Printf("✅ No Redis stats to process for yesterday")
		return nil
	}

	processedCount := 0
	errorCount := 0

	for _, key := range keys {
		// Parse the key to extract short_code
		shortCode, date, parseErr := cache.ParseDailyStatsKey(key)
		if parseErr != nil {
			log.Printf("⚠️ Skipping invalid key %s: %v", key, parseErr)
			errorCount++
			continue
		}

		// Get the click count from Redis
		clickCount, getErr := bp.redisCache.GetDailyStatsKeyValue(key)
		if getErr != nil {
			log.Printf("⚠️ Failed to get value for key %s: %v", key, getErr)
			errorCount++
			continue
		}

		if clickCount == 0 {
			// Skip zero counts
			continue
		}

		// Store in PostgreSQL daily_aggregates table
		aggregate := models.DailyAggregate{
			ShortCode:  shortCode,
			Date:       date,
			ClickCount: clickCount,
		}

		storeErr := bp.store.StoreDailyAggregate(aggregate)
		if storeErr != nil {
			log.Printf("⚠️ Failed to store daily aggregate for %s on %s: %v", shortCode, date, storeErr)
			errorCount++
			continue
		}

		// Optionally delete the Redis key (TTL will expire anyway in 14 days)
		// Uncomment the following lines if you want to delete processed keys immediately
		// if deleteErr := bp.redisCache.DeleteDailyStatsKey(key); deleteErr != nil {
		// 	log.Printf("⚠️ Failed to delete Redis key %s: %v", key, deleteErr)
		// }

		processedCount++
	}

	duration := time.Since(startTime)
	log.Printf("🎉 Redis daily stats aggregation completed!")
	log.Printf("📊 Statistics:")
	log.Printf("   • Total keys found: %d", len(keys))
	log.Printf("   • Successfully processed: %d", processedCount)
	log.Printf("   • Errors: %d", errorCount)
	log.Printf("   • Duration: %v", duration)

	return nil
}

// Close closes the batch processor connections
func (bp *BatchProcessor) Close() error {
	if bp.redisCache != nil {
		bp.redisCache.Close()
	}
	if bp.channel != nil {
		bp.channel.Close()
	}
	if bp.rabbitConn != nil {
		return bp.rabbitConn.Close()
	}
	return nil
}

func main() {
	// Get configuration from environment variables
	dbURL := getEnvOrDefault("DATABASE_URL", "postgres://user:password@localhost:5432/urlshortener?sslmode=disable")
	rabbitURL := getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	redisAddr := getEnvOrDefault("REDIS_ADDR", "localhost:6379")

	log.Printf("🚀 Starting Daily Batch Processor")
	log.Printf("📊 Database: %s", maskURL(dbURL))
	log.Printf("🐰 RabbitMQ: %s", maskURL(rabbitURL))
	log.Printf("🔴 Redis: %s", redisAddr)

	// Create batch processor
	processor, err := NewBatchProcessor(rabbitURL, dbURL, redisAddr)
	if err != nil {
		log.Fatalf("❌ Failed to create batch processor: %v", err)
	}
	defer processor.Close()

	// Process the daily batch (RabbitMQ events)
	if err := processor.ProcessDailyBatch(); err != nil {
		log.Fatalf("❌ Daily batch processing failed: %v", err)
	}

	// Process Redis daily stats aggregation
	if err := processor.ProcessRedisDailyStats(); err != nil {
		log.Printf("⚠️ Redis daily stats processing failed: %v", err)
		// Don't fail the entire job for Redis stats issues
	}

	log.Printf("✅ Daily batch processing completed successfully")
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// maskURL masks sensitive parts of URLs for logging
func maskURL(url string) string {
	if len(url) > 20 {
		return url[:10] + "***" + url[len(url)-7:]
	}
	return "***"
}
