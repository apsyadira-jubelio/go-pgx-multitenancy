package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	multitenancy "github.com/apsyadira-jubelio/go-pgx-multitenancy"
)

// TenantMiddleware extracts tenant ID from request header and adds it to the context
func TenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get tenant ID from header (in a real app, you might validate this against a tenant registry)
		tenantID := r.Header.Get("X-Tenant-ID")
		if tenantID == "" {
			http.Error(w, "Missing tenant ID", http.StatusBadRequest)
			return
		}

		// Add tenant ID to context
		ctx := multitenancy.WithTenant(r.Context(), tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// UserStore is a simple service that uses the tenant-aware database connection
type UserStore struct {
	tenantManager *multitenancy.TenantManager
	metrics       *multitenancy.MetricsCollector
	queryTracker  *multitenancy.QueryTracker
}

// User represents a user in the system
type User struct {
	ID        int    `json:"id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

func main() {
	logger := log.New(os.Stdout, "[MultiTenancy] ", log.LstdFlags)

	// Initialize tenant manager
	tenantManager, err := multitenancy.NewTenantManager(multitenancy.Config{
		DefaultDSNConfig: multitenancy.TenantDSNConfig{
			Username:         "postgres",
			Password:         "postgres",
			Host:             "localhost",
			AdditionalParams: "sslmode=disable",
		},
		// Example of tenant-specific configurations
		TenantDSNConfigs: map[string]multitenancy.TenantDSNConfig{
			// For high-traffic tenants, we might use a dedicated server
			"premium-tenant": {
				Username:         "app_user",
				Password:         "strong_password",
				Host:             "premium-db.example.com",
				Database:         "%s_data",
				AdditionalParams: "sslmode=require&pool_max_conns=20",
			},
			// Regional database servers for different tenant groups
			"europe-": { // Prefix matching for EU tenants
				Username:         "app_user",
				Password:         "eu_password",
				Host:             "eu-db.example.com",
				Database:         "%s",
				AdditionalParams: "sslmode=require",
			},
		},
		MaxConnectionsPerPool: 10,
		MinConnectionsPerPool: 2,
	})
	if err != nil {
		logger.Fatalf("Failed to initialize tenant manager: %v", err)
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
	queryTracker.AddPostHook(multitenancy.LoggingHook(logger.Printf))

	// Create user store
	userStore := &UserStore{
		tenantManager: tenantManager,
		metrics:       metrics,
		queryTracker:  queryTracker,
	}

	// Set up HTTP handlers
	mux := http.NewServeMux()

	// API endpoints with tenant middleware
	mux.Handle("/api/users", TenantMiddleware(http.HandlerFunc(userStore.HandleUsers)))
	mux.Handle("/api/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allMetrics := metrics.GetAllTenantMetrics()
		jsonResponse(w, allMetrics)
	}))

	// Start server
	logger.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		logger.Fatalf("Server error: %v", err)
	}
}

// HandleUsers handles HTTP requests for users
func (s *UserStore) HandleUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID, err := multitenancy.TenantFromContext(ctx)
	if err != nil {
		http.Error(w, "Tenant context error", http.StatusInternalServerError)
		return
	}

	// Record metrics for connection
	s.metrics.RecordConnectionAcquired(tenantID)
	defer s.metrics.RecordConnectionReleased(tenantID)

	// Get a fresh connection for this request
	// The connection pool is created on demand for this specific request
	conn, err := s.tenantManager.GetConnection(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Database connection error: %v", err), http.StatusInternalServerError)
		return
	}
	// When conn.Release() is called, the underlying pool will also be closed
	defer conn.Release()

	// Handle different HTTP methods
	switch r.Method {
	case http.MethodGet:
		var users []User

		// Execute query with tracking
		err = s.queryTracker.TrackQuery(ctx, "SELECT", "SELECT id, username, email, created_at FROM users", nil, func() error {
			// Use the wrapped connection - it works just like a normal pgx connection
			rows, err := conn.Query(ctx, "SELECT id, username, email, created_at FROM users")
			if err != nil {
				return err
			}
			defer rows.Close()

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
			http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
			return
		}

		jsonResponse(w, users)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// jsonResponse sends a JSON response
func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, fmt.Sprintf("JSON encoding error: %v", err), http.StatusInternalServerError)
	}
}
