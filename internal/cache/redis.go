package cache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis wraps the redis client
type Redis struct {
	client *redis.Client
}

// Connect creates a new Redis connection
func Connect(addr string) (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0, // default DB
	})

	// Test connection
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Redis{client: client}, nil
}

// Get retrieves a value from Redis
func (r *Redis) Get(key string) (string, error) {
	ctx := context.Background()
	return r.client.Get(ctx, key).Result()
}

// Set stores a value in Redis with TTL
func (r *Redis) Set(key, value string, ttl time.Duration) error {
	ctx := context.Background()
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Close closes the Redis connection
func (r *Redis) Close() error {
	return r.client.Close()
}

// IncrementDailyStats increments the daily stats counter for a short code
// Key format: stats:{short_code}:{YYYY-MM-DD}
// Sets TTL to 14 days only if key doesn't have TTL already
func (r *Redis) IncrementDailyStats(shortCode string, day time.Time) (int64, error) {
	ctx := context.Background()
	// Use UTC to avoid timezone/DST issues
	dayStr := day.UTC().Format("2006-01-02")
	key := fmt.Sprintf("stats:%s:%s", shortCode, dayStr)

	// Use INCR to increment the counter
	count, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	// Set TTL only if key doesn't have one (TTL == -1 means no expiration set)
	ttlResult, ttlErr := r.client.TTL(ctx, key).Result()
	if ttlErr == nil && ttlResult == -1 {
		ttl := 14 * 24 * time.Hour // 14 days
		r.client.Expire(ctx, key, ttl)
	}

	return count, nil
}

// GetDailyStats retrieves daily stats for a short code for a specific day
func (r *Redis) GetDailyStats(shortCode string, day time.Time) (int64, error) {
	ctx := context.Background()
	dayStr := day.UTC().Format("2006-01-02")
	key := fmt.Sprintf("stats:%s:%s", shortCode, dayStr)

	result, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err.Error() == "redis: nil" {
			return 0, nil // No clicks for this day
		}
		return 0, err
	}

	count, parseErr := strconv.ParseInt(result, 10, 64)
	if parseErr != nil {
		return 0, parseErr
	}
	return count, nil
}

// GetDailyStatsRange retrieves daily stats for a short code across multiple days
// Returns a map of day string (YYYY-MM-DD) to click count
func (r *Redis) GetDailyStatsRange(shortCode string, startDay, endDay time.Time) (map[string]int64, error) {
	ctx := context.Background()

	// Build list of keys to retrieve
	var keys []string
	current := startDay
	for current.Before(endDay) || current.Equal(endDay) {
		dayStr := current.UTC().Format("2006-01-02")
		key := fmt.Sprintf("stats:%s:%s", shortCode, dayStr)
		keys = append(keys, key)
		current = current.AddDate(0, 0, 1)
	}

	if len(keys) == 0 {
		return make(map[string]int64), nil
	}

	// Use MGET to retrieve all values at once
	results, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	// Build result map
	statsMap := make(map[string]int64)
	current = startDay
	for _, result := range results {
		dayStr := current.UTC().Format("2006-01-02")

		if result != nil {
			if countStr, ok := result.(string); ok {
				if count, parseErr := strconv.ParseInt(countStr, 10, 64); parseErr == nil {
					statsMap[dayStr] = count
				}
			}
		} else {
			statsMap[dayStr] = 0 // No data for this day
		}

		current = current.AddDate(0, 0, 1)
	}

	return statsMap, nil
}

// GetAllDailyStatsKeys returns all daily stats keys for a short code using SCAN
// This is useful for the batch processor to read and aggregate data
func (r *Redis) GetAllDailyStatsKeys(shortCode string) ([]string, error) {
	ctx := context.Background()
	pattern := fmt.Sprintf("stats:%s:*", shortCode)

	var keys []string
	var cursor uint64

	for {
		// Use SCAN with cursor to iterate through keys in chunks
		scanKeys, nextCursor, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}

		keys = append(keys, scanKeys...)

		// If cursor is 0, we've completed a full iteration
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	return keys, nil
}

// DeleteDailyStatsKey deletes a specific daily stats key (used after batch processing)
func (r *Redis) DeleteDailyStatsKey(key string) error {
	ctx := context.Background()
	return r.client.Del(ctx, key).Err()
}

// GetDailyStatsKeyValue gets the value for a specific daily stats key
func (r *Redis) GetDailyStatsKeyValue(key string) (int64, error) {
	ctx := context.Background()
	result, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err.Error() == "redis: nil" {
			return 0, nil
		}
		return 0, err
	}

	count, parseErr := strconv.ParseInt(result, 10, 64)
	if parseErr != nil {
		return 0, parseErr
	}
	return count, nil
}

// ParseDailyStatsKey extracts short_code and date from a daily stats key
// Key format: stats:{short_code}:{YYYY-MM-DD}
func ParseDailyStatsKey(key string) (shortCode string, date string, err error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 || parts[0] != "stats" {
		return "", "", fmt.Errorf("invalid daily stats key format: %s", key)
	}
	return parts[1], parts[2], nil
}

// GetKeysByPattern returns all keys matching the given pattern using SCAN (production-safe)
func (r *Redis) GetKeysByPattern(pattern string) ([]string, error) {
	ctx := context.Background()
	var keys []string
	var cursor uint64

	for {
		// Use SCAN with cursor to iterate through keys in chunks
		scanKeys, nextCursor, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}

		keys = append(keys, scanKeys...)

		// If cursor is 0, we've completed a full iteration
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	return keys, nil
}
