-- Initial migration for URL shortener
-- Creates the basic tables needed for the application

-- URLs table for storing shortened URLs
CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    short_code VARCHAR(10) UNIQUE NOT NULL CHECK (
        LENGTH(short_code) <= 10 AND 
        short_code ~ '^[A-Za-z0-9_-]+$'
    ),
    original_url TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE CHECK (expires_at IS NULL OR expires_at > created_at),
    is_active BOOLEAN DEFAULT true,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Simple index on short_code for redirect lookups
CREATE INDEX IF NOT EXISTS idx_urls_short_code ON urls(short_code);

-- URL clicks table for analytics (will be populated via message queue)
-- Partitioned by month for better performance with large datasets
CREATE TABLE IF NOT EXISTS url_clicks (
    id BIGSERIAL,
    url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    clicked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ip_address INET,
    user_agent TEXT,
    referer TEXT,
    PRIMARY KEY (id, clicked_at)
) PARTITION BY RANGE (clicked_at);

-- Default partition to catch all clicks until monthly partitions are added
CREATE TABLE IF NOT EXISTS url_clicks_default
  PARTITION OF url_clicks DEFAULT;

-- Composite index for "clicks over time for a URL" queries (most important)
CREATE INDEX IF NOT EXISTS idx_url_clicks_url_time ON url_clicks(url_id, clicked_at);
