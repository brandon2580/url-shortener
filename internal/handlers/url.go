package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"url-shortener/internal/cache"
	"url-shortener/internal/database"
	"url-shortener/internal/events"
	"url-shortener/internal/models"
	"url-shortener/internal/services"
)

// URLHandler handles URL-related HTTP requests
type URLHandler struct {
	db        *database.DB
	cache     *cache.Redis
	publisher *events.Publisher
}

// NewURLHandler creates a new URL handler
func NewURLHandler(db *database.DB, cache *cache.Redis, publisher *events.Publisher) *URLHandler {
	return &URLHandler{
		db:        db,
		cache:     cache,
		publisher: publisher,
	}
}

// ShortenURL handles POST /shorten requests
func (h *URLHandler) ShortenURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.ShortenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeErrorResponse(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate URL
	if !services.IsValidURL(req.LongURL) {
		h.writeErrorResponse(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	// Generate short code with retry logic for uniqueness
	var shortCode string
	var err error
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		shortCode, err = services.GenerateBase62(6)
		if err != nil {
			h.writeErrorResponse(w, "Failed to generate short code", http.StatusInternalServerError)
			return
		}

		// Try to insert into database
		err = h.db.CreateURL(shortCode, req.LongURL)
		if err == nil {
			break // Success
		}

		// If it's a unique constraint violation, retry with a new code
		if strings.Contains(err.Error(), "duplicate key") {
			continue
		}

		// Other database error
		h.writeErrorResponse(w, "Database error", http.StatusInternalServerError)
		return
	}

	if err != nil {
		h.writeErrorResponse(w, "Failed to create short URL after retries", http.StatusInternalServerError)
		return
	}

	// Cache the short_code -> original_url mapping in Redis for fast lookups
	if h.cache != nil {
		cacheErr := h.cache.Set(shortCode, req.LongURL, 24*time.Hour)
		if cacheErr != nil {
			// Log the error but don't fail the request - Redis cache is nice-to-have
			fmt.Printf("Warning: Failed to cache URL in Redis: %v\n", cacheErr)
		}
	}

	// Return success response
	response := models.ShortenResponse{
		ShortCode: shortCode,
		LongURL:   req.LongURL,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// RedirectURL handles GET /r/{short_code} requests
func (h *URLHandler) RedirectURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract short_code from URL path "/r/{short_code}"
	path := strings.TrimPrefix(r.URL.Path, "/r/")
	if path == "" || path == r.URL.Path {
		http.Error(w, "Short code required", http.StatusBadRequest)
		return
	}
	shortCode := path

	// Validate short code format
	if len(shortCode) > 10 || !services.IsValidShortCode(shortCode) {
		http.Error(w, "Invalid short code", http.StatusBadRequest)
		return
	}

	var originalURL string

	// First, try Redis cache
	if h.cache != nil {
		cachedURL, err := h.cache.Get(shortCode)
		if err == nil {
			// Cache hit! Track click and redirect immediately
			h.trackClick(r, shortCode, cachedURL)
			http.Redirect(w, r, cachedURL, http.StatusMovedPermanently)
			return
		}
		// Cache miss or Redis error, continue to database lookup
	}

	// Fallback to database lookup
	originalURL, err := h.db.GetURL(shortCode)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			http.Error(w, "Short URL not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Warm the Redis cache for next time
	if h.cache != nil {
		cacheErr := h.cache.Set(shortCode, originalURL, 24*time.Hour)
		if cacheErr != nil {
			fmt.Printf("Warning: Failed to warm Redis cache: %v\n", cacheErr)
		}
	}

	// Track the click
	h.trackClick(r, shortCode, originalURL)

	// Perform redirect
	http.Redirect(w, r, originalURL, http.StatusMovedPermanently)
}

// HealthCheck handles GET /health requests
func (h *URLHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("API service is healthy"))
}

// writeErrorResponse writes a JSON error response
func (h *URLHandler) writeErrorResponse(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(models.ErrorResponse{Error: message})
}

// trackClick handles click tracking with Redis daily counters and RabbitMQ events
func (h *URLHandler) trackClick(r *http.Request, shortCode, originalURL string) {
	now := time.Now().UTC()

	// 1. Increment Redis daily stats counter (fast, synchronous)
	if h.cache != nil {
		_, err := h.cache.IncrementDailyStats(shortCode, now)
		if err != nil {
			fmt.Printf("Warning: Failed to increment daily stats in Redis: %v\n", err)
		}
	}

	// 2. Publish click event to RabbitMQ (fast, asynchronous)
	if h.publisher != nil {
		clickEvent := services.ExtractClickEvent(r, shortCode, originalURL)
		h.publisher.PublishClickEventAsync(clickEvent)
	}
}
