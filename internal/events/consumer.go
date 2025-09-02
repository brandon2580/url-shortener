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

// Consumer handles RabbitMQ message consumption with batching
type Consumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	queue        amqp.Queue
	store        *store.ClickStore
	stopChan     chan bool
	batchSize    int
	batchTimeout time.Duration
}

// NewConsumer creates a new RabbitMQ consumer
func NewConsumer(rabbitURL string, clickStore *store.ClickStore) (*Consumer, error) {
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

	// Set QoS to process multiple messages at a time for better throughput
	err = channel.Qos(
		10,    // prefetch count - process 10 messages at once
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &Consumer{
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
func (c *Consumer) Start() error {
	log.Printf("Starting click events consumer...")

	// Start consuming messages
	msgs, err := c.channel.Consume(
		c.queue.Name, // queue
		"",           // consumer
		false,        // auto-ack (we'll manually ack after processing)
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

	log.Printf("Click events consumer started. Waiting for messages...")
	return nil
}

// processMessages processes incoming click event messages in batches
func (c *Consumer) processMessages(msgs <-chan amqp.Delivery) {
	var batch []consumerBatchItem
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
			batch = append(batch, consumerBatchItem{
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
			log.Printf("Stopping click events consumer...")
			// Process remaining batch before stopping
			if len(batch) > 0 {
				c.processBatch(batch)
			}
			return
		}
	}
}

// consumerBatchItem holds an event and its delivery for batch processing
type consumerBatchItem struct {
	event    models.ClickEvent
	delivery amqp.Delivery
}

// processBatch processes a batch of click events
func (c *Consumer) processBatch(batch []consumerBatchItem) {
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
func (c *Consumer) Stop() {
	log.Printf("Stopping click events consumer...")

	close(c.stopChan)

	// Give some time for current message processing to finish
	time.Sleep(1 * time.Second)

	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}

	log.Printf("Click events consumer stopped")
}

// GetQueueStats returns the current queue statistics
func (c *Consumer) GetQueueStats() (int, error) {
	queue, err := c.channel.QueueInspect(c.queue.Name)
	if err != nil {
		return 0, fmt.Errorf("failed to inspect queue: %w", err)
	}

	return queue.Messages, nil
}
