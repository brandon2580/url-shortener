package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"url-shortener/internal/models"
	"url-shortener/internal/store"

	"github.com/streadway/amqp"
)

// BatchConsumer handles RabbitMQ message consumption with batching
type BatchConsumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	queue        amqp.Queue
	store        *store.ClickStore
	stopChan     chan bool
	batchSize    int
	batchTimeout time.Duration
}

// NewBatchConsumer creates a new RabbitMQ batch consumer
func NewBatchConsumer(rabbitURL string, clickStore *store.ClickStore) (*BatchConsumer, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare the click events queue (in case it doesn't exist)
	queue, err := channel.QueueDeclare(
		"click_events", // queue name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Set QoS to prefetch multiple messages for batching
	err = channel.Qos(
		100,   // prefetch count - fetch up to 100 messages
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &BatchConsumer{
		conn:         conn,
		channel:      channel,
		queue:        queue,
		store:        clickStore,
		stopChan:     make(chan bool),
		batchSize:    50,              // Process 50 events at once
		batchTimeout: 5 * time.Second, // Or every 5 seconds, whichever comes first
	}, nil
}

// Start begins consuming messages from the click events queue
func (c *BatchConsumer) Start() error {
	log.Printf("Starting click events batch consumer (batch size: %d, timeout: %v)...", c.batchSize, c.batchTimeout)

	// Start consuming messages
	msgs, err := c.channel.Consume(
		c.queue.Name, // queue
		"",           // consumer
		false,        // auto-ack (we'll manually ack after batch processing)
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	// Process messages in a goroutine
	go c.processMessages(msgs)

	log.Printf("Click events batch consumer started. Waiting for messages...")
	return nil
}

// batchItem holds an event and its delivery for batch processing
type batchItem struct {
	event    models.ClickEvent
	delivery amqp.Delivery
}

// processMessages processes incoming click event messages in batches
func (c *BatchConsumer) processMessages(msgs <-chan amqp.Delivery) {
	var batch []batchItem
	ticker := time.NewTicker(c.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg := <-msgs:
			// Parse the message
			var event models.ClickEvent
			if err := json.Unmarshal(msg.Body, &event); err != nil {
				log.Printf("Warning: Failed to unmarshal click event, discarding: %v", err)
				msg.Ack(false) // Acknowledge bad message to remove it from queue
				continue
			}

			// Add to batch
			batch = append(batch, batchItem{
				event:    event,
				delivery: msg,
			})

			// Process batch if it's full
			if len(batch) >= c.batchSize {
				c.processBatch(batch)
				batch = nil                  // Reset batch
				ticker.Reset(c.batchTimeout) // Reset timer
			}

		case <-ticker.C:
			// Process batch on timeout (even if not full)
			if len(batch) > 0 {
				c.processBatch(batch)
				batch = nil // Reset batch
			}

		case <-c.stopChan:
			log.Printf("Stopping click events batch consumer...")
			// Process remaining batch before stopping
			if len(batch) > 0 {
				c.processBatch(batch)
			}
			return
		}
	}
}

// processBatch processes a batch of click events
func (c *BatchConsumer) processBatch(batch []batchItem) {
	if len(batch) == 0 {
		return
	}

	log.Printf("Processing batch of %d click events...", len(batch))

	// Extract just the events for batch processing
	events := make([]models.ClickEvent, len(batch))
	for i, item := range batch {
		events[i] = item.event
	}

	// Store all events in a single database transaction
	err := c.store.RecordClickBatch(events)

	if err != nil {
		log.Printf("Error processing batch: %v", err)
		// Nack all messages for retry
		for _, item := range batch {
			item.delivery.Nack(false, true)
		}
		return
	}

	// Acknowledge all messages on success
	for _, item := range batch {
		item.delivery.Ack(false)
	}

	log.Printf("Successfully processed batch of %d click events", len(batch))
}

// Stop gracefully stops the consumer
func (c *BatchConsumer) Stop() {
	log.Printf("Stopping click events batch consumer...")

	close(c.stopChan)

	// Give some time for current batch processing to finish
	time.Sleep(2 * time.Second)

	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}

	log.Printf("Click events batch consumer stopped")
}

// GetQueueStats returns the current queue statistics
func (c *BatchConsumer) GetQueueStats() (int, error) {
	queue, err := c.channel.QueueInspect(c.queue.Name)
	if err != nil {
		return 0, fmt.Errorf("failed to inspect queue: %w", err)
	}

	return queue.Messages, nil
}
