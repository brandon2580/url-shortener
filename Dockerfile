# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Create bin directory
RUN mkdir -p bin

# Build the API and consumer services
# Build the API service
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/api ./cmd/api

# Build the batch processor
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/batch-processor ./cmd/batch-processor
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/consumer ./cmd/consumer

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binaries from builder stage
COPY --from=builder /app/bin ./bin

# Expose port
EXPOSE 8080

# Default command (can be overridden in docker-compose)
CMD ["./bin/api"]
