package multitenancy

import (
	"sync"
	"time"
)

// MetricsCollector collects metrics about tenant database usage
type MetricsCollector struct {
	queryCounts      map[string]int64
	queryDurations   map[string]time.Duration
	connectionCounts map[string]int32
	errorCounts      map[string]int64
	mu               sync.RWMutex
}

// NewMetricsCollector creates a new MetricsCollector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		queryCounts:      make(map[string]int64),
		queryDurations:   make(map[string]time.Duration),
		connectionCounts: make(map[string]int32),
		errorCounts:      make(map[string]int64),
	}
}

// RecordQuery records a query execution for a tenant
func (mc *MetricsCollector) RecordQuery(tenantID string, duration time.Duration, success bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.queryCounts[tenantID]++
	mc.queryDurations[tenantID] += duration
	
	if !success {
		mc.errorCounts[tenantID]++
	}
}

// RecordConnectionAcquired increments the connection count for a tenant
func (mc *MetricsCollector) RecordConnectionAcquired(tenantID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.connectionCounts[tenantID]++
}

// RecordConnectionReleased decrements the connection count for a tenant
func (mc *MetricsCollector) RecordConnectionReleased(tenantID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if mc.connectionCounts[tenantID] > 0 {
		mc.connectionCounts[tenantID]--
	}
}

// GetTenantMetrics returns metrics for a specific tenant
func (mc *MetricsCollector) GetTenantMetrics(tenantID string) TenantMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var avgDuration time.Duration
	if mc.queryCounts[tenantID] > 0 {
		avgDuration = mc.queryDurations[tenantID] / time.Duration(mc.queryCounts[tenantID])
	}

	return TenantMetrics{
		TenantID:             tenantID,
		QueryCount:           mc.queryCounts[tenantID],
		ErrorCount:           mc.errorCounts[tenantID],
		ActiveConnectionCount: mc.connectionCounts[tenantID],
		AverageQueryDuration: avgDuration,
	}
}

// GetAllTenantMetrics returns metrics for all tenants
func (mc *MetricsCollector) GetAllTenantMetrics() map[string]TenantMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	result := make(map[string]TenantMetrics)
	
	// Collect all unique tenant IDs
	tenantIDs := make(map[string]bool)
	for tenantID := range mc.queryCounts {
		tenantIDs[tenantID] = true
	}
	for tenantID := range mc.connectionCounts {
		tenantIDs[tenantID] = true
	}
	
	// Create metrics for each tenant
	for tenantID := range tenantIDs {
		var avgDuration time.Duration
		if mc.queryCounts[tenantID] > 0 {
			avgDuration = mc.queryDurations[tenantID] / time.Duration(mc.queryCounts[tenantID])
		}
		
		result[tenantID] = TenantMetrics{
			TenantID:             tenantID,
			QueryCount:           mc.queryCounts[tenantID],
			ErrorCount:           mc.errorCounts[tenantID],
			ActiveConnectionCount: mc.connectionCounts[tenantID],
			AverageQueryDuration: avgDuration,
		}
	}
	
	return result
}

// TenantMetrics holds metrics information for a tenant
type TenantMetrics struct {
	TenantID             string
	QueryCount           int64
	ErrorCount           int64
	ActiveConnectionCount int32
	AverageQueryDuration time.Duration
}

// Reset resets all metrics
func (mc *MetricsCollector) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.queryCounts = make(map[string]int64)
	mc.queryDurations = make(map[string]time.Duration)
	mc.connectionCounts = make(map[string]int32)
	mc.errorCounts = make(map[string]int64)
}
