package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/coverage"
	"syscall"
	"time"

	stovekafka "github.com/trendyol/stove/go/stove-kafka"
	stoveprofiler "github.com/trendyol/stove/go/stove-profiler"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	// Ignore SIGPIPE so log writes to a closed stdout pipe don't kill the process.
	// This ensures clean shutdown (and coverage flush) when run under a process manager.
	signal.Ignore(syscall.SIGPIPE)

	profiler, err := stoveprofiler.StartFromEnv()
	if err != nil {
		log.Fatalf("failed to start Stove profiling: %v", err)
	}

	ctx := context.Background()

	port := getEnv("APP_PORT", "8080")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbName := getEnv("DB_NAME", "stove")
	dbUser := getEnv("DB_USER", "sa")
	dbPass := getEnv("DB_PASS", "sa")

	shutdownTracing, err := initTracing(ctx, "ecomm-go")
	if err != nil {
		log.Fatalf("failed to init tracing: %v", err)
	}
	defer shutdownTracing(ctx)

	connStr := fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		dbHost, dbPort, dbName, dbUser, dbPass,
	)

	db, err := initDB(connStr)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize Stove Kafka bridge (nil in production — zero overhead)
	bridge, err := stovekafka.NewBridgeFromEnv()
	if err != nil {
		log.Fatalf("failed to init stove bridge: %v", err)
	}
	defer bridge.Close()

	// Initialize Kafka producer and consumer
	brokers := getEnv("KAFKA_BROKERS", "")
	producer, stopKafka, err := initKafka(brokers, db, bridge)
	if err != nil {
		log.Fatalf("failed to init kafka: %v", err)
	}
	defer stopKafka()
	// Flush profiles before dependency cleanup, which can wait on stopped brokers.
	defer profiler.Close()

	confirmations, err := startConfirmationWorker(ctx, connStr, producer)
	if err != nil {
		log.Fatalf("failed to start confirmation worker: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := confirmations.close(stopCtx); err != nil {
			log.Printf("confirmation worker shutdown: %v", err)
		}
	}()
	checkout, fraudConn, err := newCheckoutService(db, producer, confirmations)
	if err != nil {
		log.Fatalf("failed to initialize checkout: %v", err)
	}
	defer fraudConn.Close()
	orderQueryServer, err := startOrderQueryServer(db, getEnv("ORDER_GRPC_PORT", "9091"))
	if err != nil {
		log.Fatalf("failed to start order query server: %v", err)
	}
	defer orderQueryServer.GracefulStop()
	mux := http.NewServeMux()
	registerRoutes(mux, db, producer)
	mux.HandleFunc("POST /api/orders", traced("POST /api/orders", checkout.create))
	mux.HandleFunc("GET /api/orders/{id}", traced("GET /api/orders/{id}", checkout.get))

	// Wrap with OTel HTTP instrumentation for automatic span creation
	handler := otelhttp.NewHandler(mux, "http.request")

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("Go showcase app listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-stop
	// Stove can stop dependencies before this process has finished shutting down.
	// Save the exercised counters immediately so coverage survives a forced stop.
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		if err := coverage.WriteCountersDir(dir); err != nil {
			log.Printf("writing Go coverage counters: %v", err)
		}
	}
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown error: %v", err)
	}
	log.Println("server stopped")
}
