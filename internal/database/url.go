package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// DB wraps the sql.DB connection
type DB struct {
	*sql.DB
}

// Connect creates a new database connection
func Connect(dbURL string) (*DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{db}, nil
}

// CreateURL inserts a new URL record into the database
func (db *DB) CreateURL(shortCode, originalURL string) error {
	query := `
		INSERT INTO urls (short_code, original_url, created_at, is_active) 
		VALUES ($1, $2, NOW(), true)
	`
	_, err := db.Exec(query, shortCode, originalURL)
	return err
}

// GetURL retrieves the original URL for a given short code
func (db *DB) GetURL(shortCode string) (string, error) {
	var originalURL string
	query := `
		SELECT original_url 
		FROM urls 
		WHERE short_code = $1 
		  AND is_active = true 
		  AND (expires_at IS NULL OR expires_at > NOW())
	`
	err := db.QueryRow(query, shortCode).Scan(&originalURL)
	if err != nil {
		return "", err
	}
	return originalURL, nil
}
