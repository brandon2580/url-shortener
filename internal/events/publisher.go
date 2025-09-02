package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"url-shortener/internal/models"

	"github.com/streadway/amqp"
)

// Publisher handles RabbitMQ event publishing
type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// Connect creates a new RabbitMQ connection and sets up the click events queue
func Connect(rabbitURL string) (*Publisher, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare the click events queue
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

	return &Publisher{
		conn:    conn,
		channel: channel,
		queue:   queue,
	}, nil
}

// PublishClickEvent publishes a click event to the queue
func (p *Publisher) PublishClickEvent(event models.ClickEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal click event: %w", err)
	}

	err = p.channel.Publish(
		"",           // exchange
		p.queue.Name, // routing key (queue name)
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Timestamp:   time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish click event: %w", err)
	}

	return nil
}

// PublishClickEventAsync publishes a click event asynchronously (non-blocking)
// Uses a short timeout to avoid blocking user redirects if RabbitMQ is down
func (p *Publisher) PublishClickEventAsync(event models.ClickEvent) {
	go func() {
		// Create a context with short timeout (user experience > analytics)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		done := make(chan error, 1)

		// Publish in a separate goroutine
		go func() {
			done <- p.PublishClickEvent(event)
		}()

		// Wait for either completion or timeout
		select {
		case err := <-done:
			if err != nil {
				log.Printf("Warning: Failed to publish click event: %v", err)
			}
		case <-ctx.Done():
			log.Printf("Warning: Click event publish timed out (RabbitMQ may be down)")
		}
	}()
}

// Close closes the RabbitMQ connection and channel
func (p *Publisher) Close() error {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
