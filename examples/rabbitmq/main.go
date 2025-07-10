package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	multitenancy "github.com/apsyadira-jubelio/go-pgx-multitenancy"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents the structure of messages we consume from RabbitMQ
type Message struct {
	TenantID string          `json:"tenant_id"`
	Action   string          `json:"action"`
	Payload  json.RawMessage `json:"payload"`
}

// OrderPayload represents an order data payload
type OrderPayload struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

func main() {
	// Configure logger
	logger := log.New(os.Stdout, "[RabbitMQ Consumer] ", log.LstdFlags)

	// Initialize the tenant manager with multiple database configurations
	tenantManager, err := multitenancy.NewTenantManager(multitenancy.Config{
		DefaultDSNConfig: multitenancy.TenantDSNConfig{
			Username:         "postgres",
			Password:         "postgres",
			Host:             "localhost",
			Database:         "%s_db",
			Port:             5432,
			AdditionalParams: "sslmode=disable",
		},
		TenantDSNConfigs: map[string]multitenancy.TenantDSNConfig{
			"enterprise-": {
				Username:         "enterprise_user",
				Password:         "enterprise_password",
				Host:             "enterprise-db.example.com",
				Database:         "enterprise_%s",
				AdditionalParams: "sslmode=require&pool_max_conns=10",
			},
		},
	})
	if err != nil {
		logger.Fatalf("Failed to initialize tenant manager: %v", err)
	}
	defer tenantManager.Close()

	// Initialize query tracker
	queryTracker := multitenancy.NewQueryTracker()

	// Add logging hook
	queryTracker.AddPostHook(multitenancy.LoggingHook(func(format string, args ...interface{}) {
		logger.Printf(format, args...)
	}))

	// Initialize metrics
	metrics := multitenancy.NewMetricsCollector()

	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		logger.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		logger.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare a queue
	queue, err := ch.QueueDeclare(
		"orders_queue", // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		logger.Fatalf("Failed to declare a queue: %v", err)
	}

	// Set up consumer with prefetch count
	err = ch.Qos(
		5,     // prefetch count (process 5 messages at a time)
		0,     // prefetch size (not used)
		false, // global
	)
	if err != nil {
		logger.Fatalf("Failed to set QoS: %v", err)
	}

	// Start consuming messages
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer tag (empty for auto-generation)
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		logger.Fatalf("Failed to register a consumer: %v", err)
	}

	// Signal handling for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Processing logic
	var wg sync.WaitGroup
	msgCh := make(chan amqp.Delivery, 10)

	// Consumer goroutine - spawns worker goroutines for each message
	go func() {
		for d := range msgs {
			if len(stop) > 0 {
				// Received shutdown signal, reject message for requeue
				d.Reject(true)
				continue
			}
			msgCh <- d
		}
	}()

	// Number of workers
	numWorkers := 5
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logger.Printf("Worker %d started", workerID)

			for {
				select {
				case <-stop:
					logger.Printf("Worker %d shutting down", workerID)
					return
				case d, ok := <-msgCh:
					if !ok {
						return
					}

					// Process the message
					processMessage(d, tenantManager, queryTracker, metrics, logger)
				}
			}
		}(i)
	}

	// Wait for shutdown signal
	<-stop
	logger.Println("Shutting down...")

	// Close message channel to stop workers
	close(msgCh)

	// Wait for all workers to finish
	wg.Wait()
	logger.Println("All workers have finished processing. Exiting.")
}

// processMessage handles a single message from RabbitMQ
func processMessage(d amqp.Delivery, tenantManager *multitenancy.TenantManager,
	queryTracker *multitenancy.QueryTracker, metrics *multitenancy.MetricsCollector, logger *log.Logger) {
	// Start processing timer
	start := time.Now()

	// Parse the message
	var msg Message
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		logger.Printf("Failed to parse message: %v", err)
		// Reject bad message without requeue
		d.Reject(false)
		return
	}

	logger.Printf("Processing %s action for tenant %s", msg.Action, msg.TenantID)

	// Create context with tenant ID
	ctx := multitenancy.WithTenant(context.Background(), msg.TenantID)

	// Record metrics
	metrics.RecordConnectionAcquired(msg.TenantID)
	defer metrics.RecordConnectionReleased(msg.TenantID)

	// Get a fresh connection for this message
	// A new pool is created for this specific message processing
	conn, err := tenantManager.GetConnection(ctx)
	if err != nil {
		logger.Printf("Failed to get connection for tenant %s: %v", msg.TenantID, err)
		// Requeue message for retry later
		d.Reject(true)
		return
	}
	// Will close both the connection and the pool when done
	defer conn.Release()

	// Process message based on action type
	switch msg.Action {
	case "create_order":
		// Parse order payload
		var order OrderPayload
		if err := json.Unmarshal(msg.Payload, &order); err != nil {
			logger.Printf("Failed to parse order payload: %v", err)
			d.Reject(false) // Don't requeue if payload is invalid
			return
		}

		// Store order in database with query tracking
		err = queryTracker.TrackQuery(ctx, "INSERT",
			"INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES ($1, $2, $3, NOW())",
			[]interface{}{order.OrderID, order.CustomerID, order.Amount},
			func() error {
				_, err := conn.Exec(ctx,
					"INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES ($1, $2, $3, NOW())",
					order.OrderID, order.CustomerID, order.Amount)
				return err
			})

		if err != nil {
			logger.Printf("Failed to insert order for tenant %s: %v", msg.TenantID, err)
			// Database error, requeue message
			d.Reject(true)
			return
		}

		logger.Printf("Order %s created for tenant %s", order.OrderID, msg.TenantID)

	case "update_inventory":
		// Handle inventory update
		// Similar pattern to above, but different SQL...
		logger.Printf("Processing inventory update for tenant %s", msg.TenantID)

		// Example: Simulate processing without actual DB operations
		time.Sleep(100 * time.Millisecond)

	default:
		logger.Printf("Unknown action type: %s", msg.Action)
		// Don't requeue unknown action types
		d.Reject(false)
		return
	}

	// Acknowledge message as successfully processed
	if err := d.Ack(false); err != nil {
		logger.Printf("Failed to acknowledge message: %v", err)
	}

	// Log processing time
	logger.Printf("Processed %s message for tenant %s in %v",
		msg.Action, msg.TenantID, time.Since(start))
}
