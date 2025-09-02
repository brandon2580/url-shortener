-- Daily aggregates migration
-- Creates table for storing daily click statistics aggregated from Redis

-- Daily aggregates table for efficient analytics queries
CREATE TABLE IF NOT EXISTS daily_aggregates (
    id BIGSERIAL PRIMARY KEY,
    short_code VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    click_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Unique constraint to prevent duplicate entries for the same short_code and date
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_aggregates_unique 
    ON daily_aggregates(short_code, date);

-- Index for efficient range queries on dates
CREATE INDEX IF NOT EXISTS idx_daily_aggregates_date 
    ON daily_aggregates(date);

-- Index for efficient queries by short_code
CREATE INDEX IF NOT EXISTS idx_daily_aggregates_short_code 
    ON daily_aggregates(short_code);

-- Foreign key reference to urls table
ALTER TABLE daily_aggregates 
ADD CONSTRAINT fk_daily_aggregates_urls 
FOREIGN KEY (short_code) REFERENCES urls(short_code);

-- Add trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_daily_aggregates_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_daily_aggregates_updated_at
    BEFORE UPDATE ON daily_aggregates
    FOR EACH ROW
    EXECUTE FUNCTION update_daily_aggregates_updated_at();
