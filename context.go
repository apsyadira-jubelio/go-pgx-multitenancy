package multitenancy

import (
	"context"
	"fmt"
)

type hostKey struct{}
type tenantKey struct{}

// WithHost returns a new context with the host information
func WithHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, hostKey{}, host)
}

// HostFromContext extracts the host from the context
func HostFromContext(ctx context.Context) (string, error) {
	host, ok := ctx.Value(hostKey{}).(string)
	if !ok || host == "" {
		return "", fmt.Errorf("host not found in context")
	}
	return host, nil
}

// HasHost checks if a host is present in the context
func HasHost(ctx context.Context) bool {
	host, ok := ctx.Value(hostKey{}).(string)
	return ok && host != ""
}

// WithTenant returns a new context with the tenant ID (kept for backward compatibility)
func WithTenant(ctx context.Context, host string) context.Context {
	// Also set the host for backward compatibility
	ctx = WithHost(ctx, host)
	return context.WithValue(ctx, tenantKey{}, host)
}

// TenantFromContext extracts the tenant ID from the context
// If no tenant ID is found, tries to get the host as a fallback
func TenantFromContext(ctx context.Context) (string, error) {
	host, ok := ctx.Value(tenantKey{}).(string)
	if ok && host != "" {
		return host, nil
	}

	// Try to get the host instead
	return HostFromContext(ctx)
}

// HasTenant checks if a tenant ID is present in the context
func HasTenant(ctx context.Context) bool {
	host, ok := ctx.Value(tenantKey{}).(string)
	if ok && host != "" {
		return true
	}
	return HasHost(ctx)
}
