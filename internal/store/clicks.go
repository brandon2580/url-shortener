package store

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"url-shortener/internal/models"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// ClickStore handles click analytics database operations
type ClickStore struct {
	db *sql.DB
}

// NewClickStore creates a new click store instance
func NewClickStore(db *sql.DB) *ClickStore {
	return &ClickStore{
		db: db,
	}
}

// RecordClick stores a click event in the database
func (cs *ClickStore) RecordClick(event models.ClickEvent) error {
	// First, get the URL ID from the short code
	urlID, err := cs.getURLIDByShortCode(event.ShortCode)
	if err != nil {
		return fmt.Errorf("failed to get URL ID: %w", err)
	}

	// Parse the timestamp
	clickedAt, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Insert the click record
	query := `
		INSERT INTO url_clicks (url_id, clicked_at, ip_address, user_agent, referer)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = cs.db.Exec(
		query,
		urlID,
		clickedAt,
		event.IPAddress,
		event.UserAgent,
		event.Referer,
	)
	if err != nil {
		return fmt.Errorf("failed to insert click record: %w", err)
	}

	return nil
}

// RecordClickBatch stores multiple click events in a single transaction
func (cs *ClickStore) RecordClickBatch(events []models.ClickEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Start a transaction for batch processing
	tx, err := cs.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if tx.Commit() succeeds

	// Prepare the bulk insert statement
	query := `
		INSERT INTO url_clicks (url_id, clicked_at, ip_address, user_agent, referer)
		VALUES ($1, $2, $3, $4, $5)
	`

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch statement: %w", err)
	}
	defer stmt.Close()

	// Process each event in the batch
	successCount := 0
	for _, event := range events {
		// Get URL ID for this event
		urlID, err := cs.getURLIDByShortCode(event.ShortCode)
		if err != nil {
			log.Printf("Warning: Skipping event for unknown short code %s: %v", event.ShortCode, err)
			continue
		}

		// Parse timestamp
		clickedAt, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			log.Printf("Warning: Skipping event with invalid timestamp %s: %v", event.Timestamp, err)
			continue
		}

		// Execute the prepared statement
		_, err = stmt.Exec(urlID, clickedAt, event.IPAddress, event.UserAgent, event.Referer)
		if err != nil {
			log.Printf("Warning: Failed to insert click event for %s: %v", event.ShortCode, err)
			continue
		}

		successCount++
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	log.Printf("Batch processed: %d/%d events successfully inserted", successCount, len(events))
	return nil
}

// getURLIDByShortCode retrieves the URL ID for a given short code
func (cs *ClickStore) getURLIDByShortCode(shortCode string) (int64, error) {
	var urlID int64
	query := `SELECT id FROM urls WHERE short_code = $1 AND is_active = true`

	err := cs.db.QueryRow(query, shortCode).Scan(&urlID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("URL not found for short code: %s", shortCode)
		}
		return 0, fmt.Errorf("database error: %w", err)
	}

	return urlID, nil
}

// GetClickStats returns click statistics for a URL
type ClickStats struct {
	URLId        int64      `json:"url_id"`
	ShortCode    string     `json:"short_code"`
	OriginalURL  string     `json:"original_url"`
	TotalClicks  int64      `json:"total_clicks"`
	RecentClicks int64      `json:"recent_clicks_24h"`
	FirstClick   *time.Time `json:"first_click"`
	LastClick    *time.Time `json:"last_click"`
}

// GetClickStats retrieves click statistics for a specific short code
func (cs *ClickStore) GetClickStats(shortCode string) (*ClickStats, error) {
	query := `
		SELECT 
			u.id,
			u.short_code,
			u.original_url,
			COUNT(c.id) as total_clicks,
			COUNT(CASE WHEN c.clicked_at > NOW() - INTERVAL '24 hours' THEN 1 END) as recent_clicks,
			MIN(c.clicked_at) as first_click,
			MAX(c.clicked_at) as last_click
		FROM urls u
		LEFT JOIN url_clicks c ON u.id = c.url_id
		WHERE u.short_code = $1 AND u.is_active = true
		GROUP BY u.id, u.short_code, u.original_url
	`

	var stats ClickStats
	var firstClick, lastClick sql.NullTime

	err := cs.db.QueryRow(query, shortCode).Scan(
		&stats.URLId,
		&stats.ShortCode,
		&stats.OriginalURL,
		&stats.TotalClicks,
		&stats.RecentClicks,
		&firstClick,
		&lastClick,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("URL not found for short code: %s", shortCode)
		}
		return nil, fmt.Errorf("database error: %w", err)
	}

	if firstClick.Valid {
		stats.FirstClick = &firstClick.Time
	}
	if lastClick.Valid {
		stats.LastClick = &lastClick.Time
	}

	return &stats, nil
}

// GetClickHistory returns click history for a URL with optional time range
type ClickHistoryEntry struct {
	ClickedAt time.Time `json:"clicked_at"`
	IPAddress string    `json:"ip_address"`
	UserAgent string    `json:"user_agent"`
	Referer   string    `json:"referer"`
}

// GetClickHistory retrieves click history for a short code
func (cs *ClickStore) GetClickHistory(shortCode string, limit int, offset int) ([]ClickHistoryEntry, error) {
	query := `
		SELECT c.clicked_at, c.ip_address, c.user_agent, c.referer
		FROM url_clicks c
		JOIN urls u ON c.url_id = u.id
		WHERE u.short_code = $1 AND u.is_active = true
		ORDER BY c.clicked_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := cs.db.Query(query, shortCode, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query click history: %w", err)
	}
	defer rows.Close()

	var history []ClickHistoryEntry
	for rows.Next() {
		var entry ClickHistoryEntry
		err := rows.Scan(
			&entry.ClickedAt,
			&entry.IPAddress,
			&entry.UserAgent,
			&entry.Referer,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan click history row: %w", err)
		}
		history = append(history, entry)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating click history rows: %w", err)
	}

	return history, nil
}

// GetTopURLs returns the most clicked URLs
type TopURL struct {
	ShortCode   string `json:"short_code"`
	OriginalURL string `json:"original_url"`
	TotalClicks int64  `json:"total_clicks"`
}

// GetTopURLs retrieves the most clicked URLs
func (cs *ClickStore) GetTopURLs(limit int) ([]TopURL, error) {
	query := `
		SELECT 
			u.short_code,
			u.original_url,
			COUNT(c.id) as total_clicks
		FROM urls u
		LEFT JOIN url_clicks c ON u.id = c.url_id
		WHERE u.is_active = true
		GROUP BY u.id, u.short_code, u.original_url
		HAVING COUNT(c.id) > 0
		ORDER BY total_clicks DESC
		LIMIT $1
	`

	rows, err := cs.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top URLs: %w", err)
	}
	defer rows.Close()

	var topURLs []TopURL
	for rows.Next() {
		var topURL TopURL
		err := rows.Scan(
			&topURL.ShortCode,
			&topURL.OriginalURL,
			&topURL.TotalClicks,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan top URL row: %w", err)
		}
		topURLs = append(topURLs, topURL)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating top URLs rows: %w", err)
	}

	return topURLs, nil
}

// StoreDailyAggregate stores or updates a daily aggregate record
func (cs *ClickStore) StoreDailyAggregate(aggregate models.DailyAggregate) error {
	query := `
		INSERT INTO daily_aggregates (short_code, date, click_count, created_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (short_code, date)
		DO UPDATE SET 
			click_count = daily_aggregates.click_count + EXCLUDED.click_count,
			updated_at = NOW()
	`

	dateTime, err := time.Parse("2006-01-02", aggregate.Date)
	if err != nil {
		return fmt.Errorf("invalid date format: %w", err)
	}

	_, err = cs.db.Exec(query, aggregate.ShortCode, dateTime, aggregate.ClickCount)
	if err != nil {
		return fmt.Errorf("failed to store daily aggregate: %w", err)
	}

	return nil
}

// GetDailyAggregates retrieves daily aggregates for a short code within a date range
func (cs *ClickStore) GetDailyAggregates(shortCode string, startDate, endDate time.Time) (map[string]int64, error) {
	query := `
		SELECT date, click_count
		FROM daily_aggregates
		WHERE short_code = $1 
		AND date >= $2 
		AND date <= $3
		ORDER BY date
	`

	rows, err := cs.db.Query(query, shortCode, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("failed to query daily aggregates: %w", err)
	}
	defer rows.Close()

	aggregates := make(map[string]int64)
	for rows.Next() {
		var date time.Time
		var clickCount int64

		err := rows.Scan(&date, &clickCount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan daily aggregate row: %w", err)
		}

		dateStr := date.Format("2006-01-02")
		aggregates[dateStr] = clickCount
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating daily aggregate rows: %w", err)
	}

	return aggregates, nil
}

// GetTotalClicksFromAggregates gets the total clicks from daily aggregates
func (cs *ClickStore) GetTotalClicksFromAggregates(shortCode string) (int64, error) {
	query := `
		SELECT COALESCE(SUM(click_count), 0) as total_clicks
		FROM daily_aggregates
		WHERE short_code = $1
	`

	var totalClicks int64
	err := cs.db.QueryRow(query, shortCode).Scan(&totalClicks)
	if err != nil {
		return 0, fmt.Errorf("failed to get total clicks from aggregates: %w", err)
	}

	return totalClicks, nil
}
