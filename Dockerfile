# Stage 1: Build the Go server and client binaries
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build statically-linked binaries for server and client
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o kvstore main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o kvclient client/client.go

# Stage 2: Final runtime container
FROM alpine:3.19

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/kvstore .
COPY --from=builder /app/kvclient .

# Create directory for SQLite database and WAL
RUN mkdir -p /app/data

EXPOSE 8000

# Perform healthcheck using our built-in Go CLI client
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD ["./kvclient", "--url", "localhost:8000", "health"]

CMD ["./kvstore"]
