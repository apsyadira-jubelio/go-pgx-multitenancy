package multitenancy

import (
	"context"
	"time"
)

// QueryHook is a function that gets called before and after query execution
type QueryHook func(ctx context.Context, operation string, query string, args []interface{}, startTime time.Time, err error)

// QueryTracker is a middleware that can be used to track query execution
type QueryTracker struct {
	preHooks  []QueryHook
	postHooks []QueryHook
}

// NewQueryTracker creates a new QueryTracker
func NewQueryTracker() *QueryTracker {
	return &QueryTracker{
		preHooks:  make([]QueryHook, 0),
		postHooks: make([]QueryHook, 0),
	}
}

// AddPreHook adds a hook that gets called before query execution
func (qt *QueryTracker) AddPreHook(hook QueryHook) {
	qt.preHooks = append(qt.preHooks, hook)
}

// AddPostHook adds a hook that gets called after query execution
func (qt *QueryTracker) AddPostHook(hook QueryHook) {
	qt.postHooks = append(qt.postHooks, hook)
}

// TrackQuery wraps a function that executes a query
func (qt *QueryTracker) TrackQuery(ctx context.Context, operation string, query string, args []interface{}, fn func() error) error {
	startTime := time.Now()

	// Call pre-execution hooks
	for _, hook := range qt.preHooks {
		hook(ctx, operation, query, args, startTime, nil)
	}

	err := fn()

	// Call post-execution hooks
	for _, hook := range qt.postHooks {
		hook(ctx, operation, query, args, startTime, err)
	}

	return err
}

// LoggingHook creates a hook that logs query information
func LoggingHook(logger func(format string, args ...interface{})) QueryHook {
	return func(ctx context.Context, operation string, query string, args []interface{}, startTime time.Time, err error) {
		tenantID, _ := TenantFromContext(ctx)
		duration := time.Since(startTime)

		if err != nil {
			logger("[%s] Tenant: %s, Operation: %s, Duration: %s, Error: %s, Query: %s",
				time.Now().Format(time.RFC3339), tenantID, operation, duration, err, query)
		} else {
			logger("[%s] Tenant: %s, Operation: %s, Duration: %s, Query: %s",
				time.Now().Format(time.RFC3339), tenantID, operation, duration, query)
		}
	}
}
