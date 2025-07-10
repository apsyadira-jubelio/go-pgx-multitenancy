package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	multitenancy "github.com/apsyadira-jubelio/go-pgx-multitenancy"
)

func main() {
	// Initialize the tenant manager
	tenantManager, err := multitenancy.NewTenantManager(multitenancy.Config{
		DefaultDSNConfig: multitenancy.TenantDSNConfig{
			Username:         "postgres",
			Password:         "postgres",
			Host:             "localhost",
			AdditionalParams: "sslmode=disable",
		},
		// Example of tenant-specific configuration
		TenantDSNConfigs: map[string]multitenancy.TenantDSNConfig{
			"tenant2": {
				Username:         "postgres",
				Password:         "postgres",
				Host:             "other-db-server",
				Database:         "%s_db",
				Port:             5433,
				AdditionalParams: "sslmode=disable&connect_timeout=5",
			},
		},
		MaxConnectionsPerPool: 5,
		MinConnectionsPerPool: 1,
	})
	if err != nil {
		log.Fatalf("Failed to create tenant manager: %v", err)
	}
	defer tenantManager.Close()

	// Initialize metrics collector
	metrics := multitenancy.NewMetricsCollector()

	// Setup query tracking middleware
	queryTracker := multitenancy.NewQueryTracker()
	queryTracker.AddPostHook(func(ctx context.Context, operation string, query string, args []interface{}, startTime time.Time, err error) {
		tenantID, _ := multitenancy.TenantFromContext(ctx)
		metrics.RecordQuery(tenantID, time.Since(startTime), err == nil)
	})

	// Set up logging
	logger := log.New(os.Stdout, "[MultiTenancy] ", log.LstdFlags)
	queryTracker.AddPostHook(multitenancy.LoggingHook(logger.Printf))

	// Simulate queries for different tenants
	tenants := []string{"tenant1", "tenant2", "tenant3"}

	for _, tenantID := range tenants {
		// Run in a function to manage connection lifecycle
		func(tenantID string) {
			// Create context with tenant
			ctx := multitenancy.WithTenant(context.Background(), tenantID)

			// Get a fresh connection for this request - a new pool is created on demand
			conn, err := tenantManager.GetConnection(ctx)
			if err != nil {
				logger.Printf("Failed to get connection for tenant %s: %v", tenantID, err)
				return
			}
			// When conn.Release() is called, the underlying pool will be automatically closed
			defer conn.Release()

			// Record connection metrics
			metrics.RecordConnectionAcquired(tenantID)
			defer metrics.RecordConnectionReleased(tenantID)

			// Execute query with the dynamic connection
			var result string
			err = queryTracker.TrackQuery(ctx, "SELECT", "SELECT 'Hello from ' || $1", []interface{}{tenantID}, func() error {
				return conn.QueryRow(ctx, "SELECT 'Hello from ' || $1", tenantID).Scan(&result)
			})

			if err != nil {
				logger.Printf("Failed to execute query for tenant %s: %v", tenantID, err)
				return
			}

			logger.Printf("Result from %s: %s", tenantID, result)
		}(tenantID)
	}

	// Display metrics
	fmt.Println("\n--- Tenant Metrics ---")
	for tenantID, metrics := range metrics.GetAllTenantMetrics() {
		fmt.Printf("Tenant: %s\n", tenantID)
		fmt.Printf("  Queries: %d\n", metrics.QueryCount)
		fmt.Printf("  Errors: %d\n", metrics.ErrorCount)
		fmt.Printf("  Avg Duration: %v\n", metrics.AverageQueryDuration)
		fmt.Println()
	}
}
