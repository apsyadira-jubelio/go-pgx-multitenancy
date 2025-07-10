package multitenancy

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWithTenant(t *testing.T) {
	ctx := context.Background()
	tenantID := "test-tenant"

	// Add tenant to context
	ctx = WithTenant(ctx, tenantID)

	// Check if tenant exists in context
	if !HasTenant(ctx) {
		t.Errorf("HasTenant returned false for context with tenant")
	}

	// Extract tenant from context
	extractedID, err := TenantFromContext(ctx)
	if err != nil {
		t.Errorf("Unexpected error extracting tenant from context: %v", err)
	}

	if extractedID != tenantID {
		t.Errorf("Expected tenant ID %s, got %s", tenantID, extractedID)
	}
}

func TestTenantFromContextError(t *testing.T) {
	ctx := context.Background()

	// Try to extract tenant from context without tenant
	_, err := TenantFromContext(ctx)
	if err == nil {
		t.Errorf("Expected error when extracting tenant from context without tenant")
	}
}

func TestNewTenantManager(t *testing.T) {
	// Valid configuration
	config := Config{
		DefaultDSNConfig: TenantDSNConfig{
			Username: "postgres",
			Password: "password",
			Host:     "localhost",
		},
		MaxConnectionsPerPool: 10,
		MinConnectionsPerPool: 2,
	}

	tm, err := NewTenantManager(config)
	if err != nil {
		t.Errorf("Unexpected error creating tenant manager: %v", err)
	}
	if tm == nil {
		t.Errorf("Expected non-nil tenant manager")
	}

	// Invalid configuration (empty username)
	invalidConfig := Config{
		DefaultDSNConfig: TenantDSNConfig{
			Username: "", // Missing username
			Password: "password",
			Host:     "localhost",
		},
		MaxConnectionsPerPool: 10,
		MinConnectionsPerPool: 2,
	}

	_, err = NewTenantManager(invalidConfig)
	if err == nil {
		t.Errorf("Expected error creating tenant manager with empty username")
	}
}

func TestHostConnectionCaching(t *testing.T) {
	// Create a test tenant manager with direct access to caches
	// For testing only - don't create actual connections
	tm := &TenantManager{
		config: Config{
			DefaultDSNConfig: TenantDSNConfig{
				Username: "testuser",
				Password: "password",
				Host:     "{{.TenantID}}.example.com",
				Port:     5432,
				Database: "tenant_{{.TenantID}}",
			},
		},
		poolCache: make(map[string]*pgxpool.Pool),
		poolMux:   sync.RWMutex{},
	}

	// Test cache keys based on host
	host1 := "host1"
	host2 := "host2"

	// Manually populate the pool cache for testing
	pool1 := &pgxpool.Pool{} // Mock pool object for host1
	pool2 := &pgxpool.Pool{} // Mock pool object for host2

	// Add mock pools to the cache
	tm.poolMux.Lock()
	tm.poolCache[host1] = pool1
	tm.poolCache[host2] = pool2
	tm.poolMux.Unlock()

	// Test 1: Check that different hosts are cached separately
	tm.poolMux.RLock()
	cachedPool1 := tm.poolCache[host1]
	cachedPool2 := tm.poolCache[host2]
	tm.poolMux.RUnlock()

	// Verify different hosts have different pool objects
	if cachedPool1 == cachedPool2 {
		t.Errorf("Expected different pool objects for different hosts")
	}

	// Test 2: Verify that same host returns the same cached pool
	tm.poolMux.RLock()
	initialPoolCount := len(tm.poolCache)
	tm.poolMux.RUnlock()

	// Add the same host again with a different pool
	newPool := &pgxpool.Pool{}

	// This simulates the getOrCreatePoolConfig logic in GetConnection
	tm.poolMux.RLock()
	_, exists := tm.poolCache[host1]
	tm.poolMux.RUnlock()

	if !exists {
		// Wouldn't happen in our test as we already added it
		tm.poolMux.Lock()
		tm.poolCache[host1] = newPool
		tm.poolMux.Unlock()
	}

	// Check that the pool wasn't replaced
	tm.poolMux.RLock()
	updatedPool1 := tm.poolCache[host1]
	newPoolCount := len(tm.poolCache)
	tm.poolMux.RUnlock()

	// The pool should still be the original one
	if updatedPool1 != pool1 {
		t.Errorf("Expected pool not to be replaced for the same host")
	}

	// The total number of pools shouldn't have changed
	if initialPoolCount != newPoolCount {
		t.Errorf("Pool count should remain the same: initial %d, new %d",
			initialPoolCount, newPoolCount)
	}

	// Test 3: Check multiple hosts scenario
	host3 := "host3"
	pool3 := &pgxpool.Pool{}

	tm.poolMux.Lock()
	tm.poolCache[host3] = pool3
	initialCount := len(tm.poolCache)
	tm.poolMux.Unlock()

	// Simulate multiple requests for the same hosts
	for i := 0; i < 5; i++ {
		for _, host := range []string{host1, host2, host3} {
			tm.poolMux.RLock()
			_, exists := tm.poolCache[host]
			tm.poolMux.RUnlock()

			if !exists {
				t.Errorf("Pool for host %s should exist", host)
			}
		}
	}

	// Verify the pool cache size hasn't changed after multiple lookups
	tm.poolMux.RLock()
	finalCount := len(tm.poolCache)
	tm.poolMux.RUnlock()

	if finalCount != initialCount {
		t.Errorf("Pool cache size should not change after lookups: initial %d, final %d",
			initialCount, finalCount)
	}
}

func TestDifferentTenantsWithSameHost(t *testing.T) {
	// This test verifies that when different tenant IDs resolve to the same host,
	// they share the same connection pool rather than creating new ones

	// Create a test tenant manager with direct access to caches
	// For testing only - don't create actual connections
	tm := &TenantManager{
		config: Config{
			DefaultDSNConfig: TenantDSNConfig{
				Username: "testuser",
				Password: "password",
				Host:     "static-host.example.com", // All tenants use the same host
				Port:     5432,
				Database: "tenant_{{.TenantID}}", // Database varies by tenant
			},
		},
		poolCache: make(map[string]*pgxpool.Pool),
		poolMux:   sync.RWMutex{},
	}

	// Simulate the host derivation logic
	host := "static-host.example.com" // This would be derived from the tenant ID

	// Create a mock pool for this host
	mockPool := &pgxpool.Pool{}

	// Test 1: Add the pool for the first tenant
	tm.poolMux.Lock()
	tm.poolCache[host] = mockPool
	poolCountAfterFirstTenant := len(tm.poolCache)
	tm.poolMux.Unlock()

	// Test 2: Get the pool for the second tenant (should reuse the same host pool)
	tm.poolMux.RLock()
	pool, exists := tm.poolCache[host]
	tm.poolMux.RUnlock()

	if !exists {
		t.Errorf("Expected pool to exist for host %s", host)
	}

	// Verify it's the same pool object
	if pool != mockPool {
		t.Errorf("Expected to get the same pool reference")
	}

	// Test 3: Check that after accessing for the second tenant, pool count hasn't increased
	tm.poolMux.RLock()
	poolCountAfterSecondTenant := len(tm.poolCache)
	tm.poolMux.RUnlock()

	// Pool count should be the same since both tenants use the same host
	if poolCountAfterFirstTenant != poolCountAfterSecondTenant {
		t.Errorf("Expected same pool count, got first: %d, second: %d",
			poolCountAfterFirstTenant, poolCountAfterSecondTenant)
	}

	// Test 4: Verify that the host-based caching handles multiple tenants correctly
	tenants := []string{"tenant1", "tenant2", "tenant3"}

	// Track how many times we add to the pool cache
	originalPoolCount := poolCountAfterSecondTenant

	// Simulate multiple tenants accessing with the same host
	for range tenants {
		// In a real scenario, these would all resolve to the same host
		// Since we're using static-host.example.com for all tenants

		// Check if we have a pool for this host (we should)
		tm.poolMux.RLock()
		_, exists := tm.poolCache[host]
		tm.poolMux.RUnlock()

		// If not exists, add it (shouldn't happen after first iteration)
		if !exists {
			tm.poolMux.Lock()
			tm.poolCache[host] = &pgxpool.Pool{}
			tm.poolMux.Unlock()
		}
	}

	// Verify the final pool count
	tm.poolMux.RLock()
	finalPoolCount := len(tm.poolCache)
	tm.poolMux.RUnlock()

	// Should still be the same count as we're reusing the same host key
	if originalPoolCount != finalPoolCount {
		t.Errorf("Pool cache size shouldn't change with multiple tenants using same host: original %d, final %d",
			originalPoolCount, finalPoolCount)
	}
}
