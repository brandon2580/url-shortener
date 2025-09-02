package models

// ShortenRequest represents the JSON payload for URL shortening
type ShortenRequest struct {
	LongURL string `json:"long_url"`
}

// ShortenResponse represents the JSON response for successful URL shortening
type ShortenResponse struct {
	ShortCode string `json:"short_code"`
	LongURL   string `json:"long_url"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// URL represents a URL record in the database
type URL struct {
	ID          int64   `json:"id"`
	ShortCode   string  `json:"short_code"`
	OriginalURL string  `json:"original_url"`
	CreatedAt   string  `json:"created_at"`
	ExpiresAt   *string `json:"expires_at,omitempty"`
	IsActive    bool    `json:"is_active"`
	UpdatedAt   *string `json:"updated_at,omitempty"`
}

// ClickEvent represents a click event for analytics
type ClickEvent struct {
	ShortCode   string `json:"short_code"`
	OriginalURL string `json:"original_url"`
	IPAddress   string `json:"ip_address"`
	UserAgent   string `json:"user_agent"`
	Referer     string `json:"referer"`
	Timestamp   string `json:"timestamp"`
}

// DailyAggregate represents daily click statistics
type DailyAggregate struct {
	ShortCode  string `json:"short_code"`
	Date       string `json:"date"` // YYYY-MM-DD format
	ClickCount int64  `json:"click_count"`
}

// ClickStatsResponse represents the response for analytics stats
type ClickStatsResponse struct {
	URLInfo    URLInfo          `json:"url_info"`
	ClickStats ClickStats       `json:"click_stats"`
	DailyStats map[string]int64 `json:"daily_stats,omitempty"`
}

// URLInfo represents basic URL information
type URLInfo struct {
	ShortCode   string `json:"short_code"`
	OriginalURL string `json:"original_url"`
}

// ClickStats represents aggregated click statistics
type ClickStats struct {
	TotalClicksPostgres int64  `json:"total_clicks_postgres"`
	TotalClicksRedis    int64  `json:"total_clicks_redis"`
	RecentClicks24h     int64  `json:"recent_clicks_24h"`
	FirstClick          string `json:"first_click,omitempty"`
	LastClick           string `json:"last_click,omitempty"`
}
