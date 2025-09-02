package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"url-shortener/internal/cache"
	"url-shortener/internal/models"
	"url-shortener/internal/store"
)

// AnalyticsHandler handles analytics-related HTTP requests
type AnalyticsHandler struct {
	clickStore *store.ClickStore
	cache      *cache.Redis
}

// NewAnalyticsHandler creates a new analytics handler
func NewAnalyticsHandler(clickStore *store.ClickStore, cache *cache.Redis) *AnalyticsHandler {
	return &AnalyticsHandler{
		clickStore: clickStore,
		cache:      cache,
	}
}

// GetClickStats handles GET /analytics/stats/{short_code} requests
func (h *AnalyticsHandler) GetClickStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract short_code from URL path "/analytics/stats/{short_code}"
	path := strings.TrimPrefix(r.URL.Path, "/analytics/stats/")
	if path == "" || path == r.URL.Path {
		http.Error(w, "Short code required", http.StatusBadRequest)
		return
	}
	shortCode := path

	// Parse query parameters for date range
	query := r.URL.Query()
	daysBack := 7 // default to 7 days
	if d := query.Get("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed > 0 && parsed <= 365 {
			daysBack = parsed
		}
	}

	// Get PostgreSQL stats for basic URL info
	stats, err := h.clickStore.GetClickStats(shortCode)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "URL not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get click stats", http.StatusInternalServerError)
		return
	}

	// Combine Redis (hot) and PostgreSQL (cold) data
	response, err := h.combineStatsData(shortCode, stats, daysBack)
	if err != nil {
		http.Error(w, "Failed to combine analytics data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// combineStatsData combines recent Redis data with historical PostgreSQL data
func (h *AnalyticsHandler) combineStatsData(shortCode string, pgStats *store.ClickStats, daysBack int) (*models.ClickStatsResponse, error) {
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -daysBack)

	// Get Redis daily stats for recent days (within TTL)
	var redisStats map[string]int64
	var redisTotal int64
	if h.cache != nil {
		var err error
		redisStats, err = h.cache.GetDailyStatsRange(shortCode, startDate, now)
		if err != nil {
			log.Printf("Warning: Failed to get Redis daily stats: %v", err)
			redisStats = make(map[string]int64)
		}

		// Calculate total from Redis daily stats
		for _, count := range redisStats {
			redisTotal += count
		}
	} else {
		redisStats = make(map[string]int64)
	} // Get PostgreSQL daily aggregates for historical data
	pgAggregates, err := h.clickStore.GetDailyAggregates(shortCode, startDate, now)
	if err != nil {
		log.Printf("Warning: Failed to get PostgreSQL daily aggregates: %v", err)
		pgAggregates = make(map[string]int64)
	}

	// Combine both data sources, preferring Redis for recent days
	combinedDailyStats := make(map[string]int64)
	current := startDate
	for current.Before(now) || current.Equal(now) {
		dayStr := current.Format("2006-01-02")

		// Prefer Redis data if available, fallback to PostgreSQL
		if redisCount, hasRedis := redisStats[dayStr]; hasRedis && redisCount > 0 {
			combinedDailyStats[dayStr] = redisCount
		} else if pgCount, hasPG := pgAggregates[dayStr]; hasPG {
			combinedDailyStats[dayStr] = pgCount
		} else {
			combinedDailyStats[dayStr] = 0
		}

		current = current.AddDate(0, 0, 1)
	}

	// Calculate recent clicks (last 24 hours)
	yesterday := now.AddDate(0, 0, -1)
	recentClicks := int64(0)
	if redisCount, hasRedis := redisStats[now.Format("2006-01-02")]; hasRedis {
		recentClicks += redisCount
	}
	if yesterdayCount, hasYesterday := redisStats[yesterday.Format("2006-01-02")]; hasYesterday {
		recentClicks += yesterdayCount
	}

	// Format timestamps
	var firstClick, lastClick string
	if pgStats.FirstClick != nil {
		firstClick = pgStats.FirstClick.Format(time.RFC3339)
	}
	if pgStats.LastClick != nil {
		lastClick = pgStats.LastClick.Format(time.RFC3339)
	}

	return &models.ClickStatsResponse{
		URLInfo: models.URLInfo{
			ShortCode:   pgStats.ShortCode,
			OriginalURL: pgStats.OriginalURL,
		},
		ClickStats: models.ClickStats{
			TotalClicksPostgres: pgStats.TotalClicks,
			TotalClicksRedis:    redisTotal,
			RecentClicks24h:     recentClicks,
			FirstClick:          firstClick,
			LastClick:           lastClick,
		},
		DailyStats: combinedDailyStats,
	}, nil
}

// GetDailyStatsEndpoint handles GET /analytics/daily/{short_code} requests
func (h *AnalyticsHandler) GetDailyStatsEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract short_code from URL path "/analytics/daily/{short_code}"
	path := strings.TrimPrefix(r.URL.Path, "/analytics/daily/")
	if path == "" || path == r.URL.Path {
		http.Error(w, "Short code required", http.StatusBadRequest)
		return
	}
	shortCode := path

	// Parse query parameters for date range
	query := r.URL.Query()
	daysBack := 7 // default to 7 days
	if d := query.Get("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed > 0 && parsed <= 365 {
			daysBack = parsed
		}
	}

	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -daysBack)

	// Get Redis daily stats for recent days
	var redisStats map[string]int64
	if h.cache != nil {
		var err error
		redisStats, err = h.cache.GetDailyStatsRange(shortCode, startDate, now)
		if err != nil {
			log.Printf("Warning: Failed to get Redis daily stats: %v", err)
			redisStats = make(map[string]int64)
		}
	} else {
		redisStats = make(map[string]int64)
	}

	// Get PostgreSQL daily aggregates for historical data
	pgAggregates, err := h.clickStore.GetDailyAggregates(shortCode, startDate, now)
	if err != nil {
		log.Printf("Warning: Failed to get PostgreSQL daily aggregates: %v", err)
		pgAggregates = make(map[string]int64)
	}

	// Combine both data sources, preferring Redis for recent days
	combinedDailyStats := make(map[string]int64)
	current := startDate
	for current.Before(now) || current.Equal(now) {
		dayStr := current.Format("2006-01-02")

		// Prefer Redis data if available, fallback to PostgreSQL
		if redisCount, hasRedis := redisStats[dayStr]; hasRedis && redisCount > 0 {
			combinedDailyStats[dayStr] = redisCount
		} else if pgCount, hasPG := pgAggregates[dayStr]; hasPG {
			combinedDailyStats[dayStr] = pgCount
		} else {
			combinedDailyStats[dayStr] = 0
		}

		current = current.AddDate(0, 0, 1)
	}

	response := map[string]interface{}{
		"short_code":  shortCode,
		"days_back":   daysBack,
		"start_date":  startDate.Format("2006-01-02"),
		"end_date":    now.Format("2006-01-02"),
		"daily_stats": combinedDailyStats,
		"data_sources": map[string]interface{}{
			"redis_keys":    len(redisStats),
			"postgres_keys": len(pgAggregates),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GetClickHistory handles GET /analytics/history/{short_code} requests
func (h *AnalyticsHandler) GetClickHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract short_code from URL path "/analytics/history/{short_code}"
	path := strings.TrimPrefix(r.URL.Path, "/analytics/history/")
	if path == "" || path == r.URL.Path {
		http.Error(w, "Short code required", http.StatusBadRequest)
		return
	}
	shortCode := path

	// Parse query parameters
	query := r.URL.Query()
	limit := 10 // default
	offset := 0 // default

	if l := query.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	if o := query.Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	// Get click history
	history, err := h.clickStore.GetClickHistory(shortCode, limit, offset)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "URL not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get click history", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"short_code":    shortCode,
		"limit":         limit,
		"offset":        offset,
		"click_count":   len(history),
		"click_history": history,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GetTopURLs handles GET /analytics/top requests
func (h *AnalyticsHandler) GetTopURLs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	query := r.URL.Query()
	limit := 10 // default

	if l := query.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 50 {
			limit = parsed
		}
	}

	// Get top URLs
	topURLs, err := h.clickStore.GetTopURLs(limit)
	if err != nil {
		http.Error(w, "Failed to get top URLs", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"limit":    limit,
		"top_urls": topURLs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GetQueueStatus handles GET /analytics/queue-status requests
// This provides visibility into the RabbitMQ queue processing status
func (h *AnalyticsHandler) GetQueueStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// For now, return basic info - in production this could connect to RabbitMQ management API
	response := map[string]interface{}{
		"message": "Queue status endpoint - connect to RabbitMQ management API for detailed metrics",
		"tip":     "Access RabbitMQ management at http://localhost:15672 (guest/guest)",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
