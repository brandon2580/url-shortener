package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"

	"url-shortener/internal/cache"
	"url-shortener/internal/database"
	"url-shortener/internal/events"
	"url-shortener/internal/handlers"
	"url-shortener/internal/store"

	_ "github.com/lib/pq" // PostgreSQL driver
)

func main() {
	// Get environment variables
	dbURL := os.Getenv("DB_URL")
	redisAddr := os.Getenv("REDIS_ADDR")
	rabbitURL := os.Getenv("RABBIT_URL")

	fmt.Printf("Starting API service...\n")
	fmt.Printf("DB_URL: %s\n", dbURL)
	fmt.Printf("REDIS_ADDR: %s\n", redisAddr)
	fmt.Printf("RABBIT_URL: %s\n", rabbitURL)

	// Connect to database
	db, err := database.Connect(dbURL)
	if err != nil {
		fmt.Printf("Database connection failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Println("Connected to database successfully")

	// Connect to PostgreSQL for analytics (using sql.DB directly for store package)
	sqlDB, err := sql.Open("postgres", dbURL)
	if err != nil {
		fmt.Printf("Failed to create SQL DB connection: %v\n", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	if err := sqlDB.Ping(); err != nil {
		fmt.Printf("Failed to ping SQL database: %v\n", err)
		os.Exit(1)
	}

	// Connect to Redis
	var redisCache *cache.Redis
	redisCache, err = cache.Connect(redisAddr)
	if err != nil {
		fmt.Printf("Warning: Failed to connect to Redis: %v\n", err)
		fmt.Println("Continuing without Redis cache...")
		redisCache = nil
	} else {
		defer redisCache.Close()
		fmt.Println("Connected to Redis successfully")
	}

	// Connect to RabbitMQ
	var eventPublisher *events.Publisher
	eventPublisher, err = events.Connect(rabbitURL)
	if err != nil {
		fmt.Printf("Warning: Failed to connect to RabbitMQ: %v\n", err)
		fmt.Println("Continuing without click event publishing...")
		eventPublisher = nil
	} else {
		defer eventPublisher.Close()
		fmt.Println("Connected to RabbitMQ successfully")
	}

	// Create stores
	clickStore := store.NewClickStore(sqlDB)

	// Create handlers
	urlHandler := handlers.NewURLHandler(db, redisCache, eventPublisher)
	analyticsHandler := handlers.NewAnalyticsHandler(clickStore, redisCache)

	// Register routes
	http.HandleFunc("/shorten", urlHandler.ShortenURL)
	http.HandleFunc("/r/", urlHandler.RedirectURL)
	http.HandleFunc("/health", urlHandler.HealthCheck)

	// Analytics endpoints
	http.HandleFunc("/analytics/stats/", analyticsHandler.GetClickStats)
	http.HandleFunc("/analytics/daily/", analyticsHandler.GetDailyStatsEndpoint)
	http.HandleFunc("/analytics/history/", analyticsHandler.GetClickHistory)
	http.HandleFunc("/analytics/top", analyticsHandler.GetTopURLs)
	http.HandleFunc("/analytics/queue-status", analyticsHandler.GetQueueStatus)

	fmt.Println("API service starting on port 8080...")
	fmt.Println("POST /shorten - Create short URL")
	fmt.Println("GET /r/{short_code} - Redirect to original URL (with click tracking)")
	fmt.Println("GET /health - Health check")
	fmt.Println("GET /analytics/stats/{short_code} - Get click statistics")
	fmt.Println("GET /analytics/daily/{short_code} - Get daily click stats (Redis + Postgres)")
	fmt.Println("GET /analytics/history/{short_code} - Get click history")
	fmt.Println("GET /analytics/top - Get top clicked URLs")
	fmt.Println("GET /analytics/queue-status - Get queue processing status")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("API service failed to start: %v\n", err)
	}
}
