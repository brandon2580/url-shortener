package services

import (
	"crypto/rand"
	"net/http"
	"net/url"
	"time"

	"url-shortener/internal/models"
)

const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// GenerateBase62 generates a random base62 string of the given length
func GenerateBase62(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = base62Chars[int(bytes[i])%len(base62Chars)]
	}
	return string(result), nil
}

// IsValidURL validates if a string is a proper URL
func IsValidURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return u.Scheme != "" && u.Host != ""
}

// IsValidShortCode validates if a short code contains only allowed characters
func IsValidShortCode(code string) bool {
	for _, char := range code {
		if !((char >= '0' && char <= '9') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= 'a' && char <= 'z') ||
			char == '-' || char == '_') {
			return false
		}
	}
	return true
}

// ExtractClickEvent creates a ClickEvent from HTTP request data
func ExtractClickEvent(r *http.Request, shortCode, originalURL string) models.ClickEvent {
	// Extract IP address (handle proxies and remove port)
	ipAddress := r.RemoteAddr
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		ipAddress = forwarded
	} else if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		ipAddress = realIP
	}

	// Remove port from IP address if present (e.g., "192.168.1.1:45678" -> "192.168.1.1")
	if lastColon := lastIndex(ipAddress, ':'); lastColon >= 0 {
		// Check if this might be IPv6 (multiple colons) vs IPv4 with port (single colon)
		if countChar(ipAddress, ':') == 1 {
			ipAddress = ipAddress[:lastColon]
		}
	}

	// Extract User-Agent
	userAgent := r.Header.Get("User-Agent")
	if userAgent == "" {
		userAgent = "Unknown"
	}

	// Extract Referer
	referer := r.Header.Get("Referer")
	if referer == "" {
		referer = "Direct"
	}

	return models.ClickEvent{
		ShortCode:   shortCode,
		OriginalURL: originalURL,
		IPAddress:   ipAddress,
		UserAgent:   userAgent,
		Referer:     referer,
		Timestamp:   time.Now().Format(time.RFC3339),
	}
}

// Helper function to find last index of a character
func lastIndex(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// Helper function to count character occurrences
func countChar(s string, c byte) int {
	count := 0
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			count++
		}
	}
	return count
}
