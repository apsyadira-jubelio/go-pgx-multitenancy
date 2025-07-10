# go-pgx-multitenancy

A Go library for managing multi-tenant PostgreSQL connections using pgx and pgxpool.

## Features

- Multi-tenant database connection management
- Dynamic per-request connections to different database hosts
- Support for multiple database hostnames across tenants
- Context-based tenant determination for each HTTP request
- Automatic connection cleanup
- Connection middleware support
- Connection metrics

## Installation

```bash
go get github.com/apsyadira-jubelio/go-pgx-multitenancy
```

## Usage

```go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	multitenancy "github.com/apsyadira-jubelio/go-pgx-multitenancy"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// User represents a database user entity
type User struct {
	ID        int         `json:"id"`
	Username  string      `json:"username"`
	Email     *string     `json:"email"`
	CreatedAt pgtype.Date `json:"created_at"`
}

func main() {
	// Configure logger
	logger := log.New(os.Stdout, "[Fiber Example] ", log.LstdFlags)

	// Initialize the tenant manager with multiple database configurations
	tenantManager, err := multitenancy.NewTenantManager(multitenancy.Config{
		DefaultDSNConfig: multitenancy.TenantDSNConfig{
			Username:        "",
			Password:        "",
			Database:        "",
			Port:            5432,
			ApplicationName: "fiber",
			Timezone:        "Asia/Jakarta",
		},
		MaxConnectionsPerPool: 50,
		MinConnectionsPerPool: 1,
		MaxConnectionIdleTime: 5 * time.Second,
		DefaultQueryExecMode:  pgx.QueryExecModeDescribeExec,
	})

	if err != nil {
		logger.Fatalf("Failed to initialize tenant manager: %v", err)
	}

	defer tenantManager.Close()

	// Initialize query tracker for monitoring queries
	queryTracker := multitenancy.NewQueryTracker()

	// Add logging hook for query monitoring
	queryTracker.AddPostHook(multitenancy.LoggingHook(func(format string, args ...interface{}) {
		logger.Printf(format, args...)
	}))

	// Initialize metrics collector
	metrics := multitenancy.NewMetricsCollector()

	// Create Fiber app
	app := fiber.New()
	// Define a connection middleware to handle DB connections for each request
	// This middleware creates a connection at the start of a request and ensures it's released at the end
	app.Use(func(c *fiber.Ctx) error {
		// assume host is localhost get from session cache redis
		host := "localhost"
		c.Locals("host", host)

		// Create Go context with Host
		ctx := multitenancy.WithHost(c.Context(), host)

		// Get a fresh connection for this request
		conn, err := tenantManager.GetConnection(ctx)
		if err != nil {
			logger.Printf("Failed to get connection for tenant %s: %v", host, err)
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Database connection error: %v", err))
		}

		// Important: Store connection in context for this request
		c.Locals("connection", conn)

		// Track metrics
		metrics.RecordConnectionAcquired(host)

		// Process the request
		err = c.Next()

		// After request completes, explicitly release the connection
		if connObj := c.Locals("connection"); connObj != nil {
			if conn, ok := connObj.(*multitenancy.TenantConn); ok {
				// Explicitly release the connection back to the pool or close it
				conn.Release()
				logger.Printf("Connection for host %s explicitly released after request", host)
				metrics.RecordConnectionReleased(host)
			}
		}

		// Return the error from Next() if any
		return err
	})

	app.Get("/metrics", func(c *fiber.Ctx) error {
		return c.JSON(metrics.GetTenantMetrics("localhost"))
	})

	// API routes
	app.Get("/api/users", func(c *fiber.Ctx) error {
		ctx := c.Context()
		conn := c.Locals("connection").(*multitenancy.TenantConn)

		// Query users with tracking
		var users []User
		err = queryTracker.TrackQuery(ctx, "SELECT", "SELECT id, username, email, created_at FROM users", nil, func() error {
			rows, err := conn.Conn.Query(ctx, "SELECT id, username, email, created_at FROM users")
			if err != nil {
				return err
			}
			defer rows.Close()

			// Scan rows into User structs
			for rows.Next() {
				var user User
				if err := rows.Scan(&user.ID, &user.Username, &user.Email, &user.CreatedAt); err != nil {
					return err
				}
				users = append(users, user)
			}

			return rows.Err()
		})

		if err != nil {
			logger.Printf("Query error for tenant %s: %v", "localhost", err)
			return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Query error: %v", err))
		}

		// Return users as JSON
		return c.JSON(users)
	})

	// Start server
	logger.Printf("Fiber server starting on :3000")
	logger.Fatal(app.Listen(":3000"))
}

```

## License

## How It Works

`go-pgx-multitenancy` uses host-based connection pool caching to efficiently manage database connections:

1. **Request Arrives**: When a request comes in, host information is extracted from the request context (typically derived from tenant ID)

2. **Host-Based Connection Caching**:

   - If a connection pool already exists for the host, it's reused
   - If no pool exists for the host, a new pool is created and cached for future requests

3. **Connection Acquisition**: A connection is acquired from the host's pool for the specific request

4. **Automatic Connection Release**: When the request is complete, the connection is released back to the pool

5. **Pool Lifecycle Management**: All cached pools are properly closed when the application shuts down

## Benefits

- **Improved Performance**: Connection pools are reused for the same host, reducing overhead
- **Resource Efficiency**: Prevents duplicate connection pools to the same database host
- **Dynamic Host Selection**: Different tenants can connect to different database servers
- **Simplified Management**: Connection pools are automatically created and cached by host
- **Memory Leak Prevention**: Proper cleanup of all pools during application shutdown
- **Multi-Tenant Isolation**: Each host gets its own dedicated connection pool

MIT
