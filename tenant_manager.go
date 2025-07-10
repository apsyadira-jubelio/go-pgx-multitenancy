package multitenancy

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TenantDSNConfig defines how to construct a DSN for a specific tenant
type TenantDSNConfig struct {
	// Host is the PostgreSQL host
	// Example: "localhost"
	Host string

	// Database is the database name
	Database string

	// Port is the PostgreSQL port to use (defaults to 5432 if not specified)
	Port int

	// Username for the PostgreSQL connection
	Username string

	// Password for the PostgreSQL connection
	Password string

	// AdditionalParams are extra connection parameters
	// Example: "sslmode=disable&connect_timeout=10"
	AdditionalParams string

	// ApplicationName is the application name to use for the connection
	ApplicationName string

	// Timezone is the timezone to use for the connection
	Timezone string
}

// Config holds the configuration for the TenantManager
type Config struct {
	// DefaultDSNConfig is the default DSN configuration used when tenant-specific
	// configuration is not provided
	DefaultDSNConfig TenantDSNConfig

	// TenantDSNConfigs maps tenant IDs to specific DSN configurations
	// Use this to override settings for specific tenants
	TenantDSNConfigs map[string]TenantDSNConfig

	// MaxConnectionsPerPool is the maximum number of connections per tenant pool
	MaxConnectionsPerPool int32

	// MinConnectionsPerPool is the minimum number of connections per tenant pool
	MinConnectionsPerPool int32

	MaxConnectionIdleTime time.Duration

	DefaultQueryExecMode pgx.QueryExecMode
}

// TenantManager manages connections for multiple tenants
type TenantManager struct {
	config Config
	// Single cache for connection pools keyed by host
	poolCache map[string]*pgxpool.Pool
	poolMux   sync.RWMutex
}

// NewTenantManager creates a new TenantManager with the given configuration
func NewTenantManager(config Config) (*TenantManager, error) {
	// Validate required configuration
	if config.DefaultDSNConfig.Username == "" {
		return nil, fmt.Errorf("username is required")
	}

	// Set reasonable defaults if not provided
	if config.MaxConnectionsPerPool == 0 {
		config.MaxConnectionsPerPool = 5
	}
	if config.MinConnectionsPerPool == 0 {
		config.MinConnectionsPerPool = 1
	}
	if config.MaxConnectionIdleTime == 0 {
		config.MaxConnectionIdleTime = 5 * time.Minute
	}

	// Initialize a new TenantManager
	tm := &TenantManager{
		config:    config,
		poolCache: make(map[string]*pgxpool.Pool),
	}

	return tm, nil
}

// TenantConn is a wrapper around pgxpool.Conn that adds automatic pool closing
type TenantConn struct {
	*pgxpool.Conn
	pool   *pgxpool.Pool
	cached bool // Indicates if the connection is from a cached pool
}

// Release releases the connection back to the pool
// If the connection is not from a cached pool, it also closes the pool
func (tc *TenantConn) Release() {
	// Check if connection is healthy before releasing
	if tc.Conn != nil {
		// Force the connection to complete any pending operations
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		
		// Try to ping the connection to verify it's healthy
		err := tc.Conn.Ping(ctx)
		if err != nil {
			log.Printf("Unhealthy connection detected during release, closing forcefully: %v", err)
		}
		
		// Release the connection back to the pool
		tc.Conn.Release()
	}

	// Only close the pool if it's not cached
	if !tc.cached && tc.pool != nil {
		log.Println("Closing non-cached connection pool")
		tc.pool.Close()
	}
}

// GetConnection returns a connection for the tenant in the context
func (tm *TenantManager) GetConnection(ctx context.Context) (*TenantConn, error) {
	// Get tenant from context
	host, err := TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}

	log.Println("GetConnection for host:", host)

	// Check if we already have a connection pool for this host
	tm.poolMux.RLock()
	pool, exists := tm.poolCache[host]
	tm.poolMux.RUnlock()

	if !exists {
		// No existing pool, create a new one
		poolConfig, err := tm.getOrCreatePoolConfig(host)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool config for tenant %s: %w", host, err)
		}

		// Create the new pool
		newPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection for tenant %s: %w", host, err)
		}

		// Store the pool in the cache
		tm.poolMux.Lock()
		// Check again to avoid race conditions
		if existingPool, doubleCheck := tm.poolCache[host]; doubleCheck {
			// Another goroutine created the pool while we were waiting for the lock
			tm.poolMux.Unlock()
			// Close the pool we just created since we won't use it
			newPool.Close()
			pool = existingPool
		} else {
			// Store our newly created pool in the pool cache
			tm.poolCache[host] = newPool
			pool = newPool
			tm.poolMux.Unlock()
		}
	}

	// Get a connection from the pool
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection for tenant %s: %w", host, err)
	}

	// Wrap the connection with our TenantConn to handle cleanup
	// The connection is returned to the pool when released, but the pool itself remains open
	tenantConn := &TenantConn{
		Conn:   conn,
		pool:   pool,
		cached: true,
	}

	return tenantConn, nil
}

// buildDSN constructs a DSN string for the specified tenant
func (tm *TenantManager) buildDSN(host string) string {
	// Check if we have a specific config for this tenant
	config, exists := tm.config.TenantDSNConfigs[host]
	if !exists {
		// Use default config
		config = tm.config.DefaultDSNConfig
	}

	// Determine port
	port := config.Port
	if port == 0 {
		port = 5432 // Default PostgreSQL port
	}

	// Construct the DSN
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		config.Username,
		config.Password,
		host,
		config.Port,
		config.Database)

	// Add additional parameters if provided
	if config.AdditionalParams != "" {
		dsn += "?" + config.AdditionalParams
	}

	return dsn
}

// getOrCreatePoolConfig creates a connection pool configuration for the specified tenant
func (tm *TenantManager) getOrCreatePoolConfig(tenantID string) (*pgxpool.Config, error) {
	// Create a new config directly - we no longer cache configs separately
	dsn := tm.buildDSN(tenantID)
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	// Set pool configuration based on TenantManager settings
	poolConfig.MaxConns = tm.config.MaxConnectionsPerPool
	poolConfig.MinConns = tm.config.MinConnectionsPerPool
	
	// Configure idle connection management
	if tm.config.MaxConnectionIdleTime > 0 {
		poolConfig.MaxConnIdleTime = tm.config.MaxConnectionIdleTime
	} else {
		// Default to 30 seconds if not specified
		poolConfig.MaxConnIdleTime = 30 * time.Second
	}
	
	// Set health check interval to detect and cleanup broken connections
	poolConfig.HealthCheckPeriod = 15 * time.Second
	
	// Set connection lifetime to recycle connections periodically
	poolConfig.MaxConnLifetime = 5 * time.Minute
	
	// Set query execution mode
	poolConfig.ConnConfig.DefaultQueryExecMode = tm.config.DefaultQueryExecMode
	
	// Set runtime parameters
	poolConfig.ConnConfig.RuntimeParams = map[string]string{}
	
	// Use application name if provided, otherwise use database name
	appName := tm.config.DefaultDSNConfig.ApplicationName
	if appName == "" {
		appName = tm.config.DefaultDSNConfig.Database
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = appName
	
	// Set timezone if provided
	if tm.config.DefaultDSNConfig.Timezone != "" {
		poolConfig.ConnConfig.RuntimeParams["timezone"] = tm.config.DefaultDSNConfig.Timezone
	}

	return poolConfig, nil
}

// Close cleans up any resources used by this TenantManager
func (tm *TenantManager) Close() {
	// Close all cached connection pools to prevent resource leaks
	tm.poolMux.Lock()
	for _, pool := range tm.poolCache {
		pool.Close()
	}
	// Clear the pool cache
	tm.poolCache = make(map[string]*pgxpool.Pool)
	tm.poolMux.Unlock()
}
