package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	stovekafka "github.com/trendyol/stove/go/stove-kafka"
)

const (
	topicProductCreated = "product.created"
	topicProductUpdate  = "product.update"
)

// ProductCreatedEvent is published to the product.created topic when a product is created.
type ProductCreatedEvent struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// ProductUpdateEvent is consumed from the product.update topic to update existing products.
type ProductUpdateEvent struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// KafkaProducer separates message production from the checkout and worker code.
type KafkaProducer interface {
	SendMessage(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	Close() error
}

// initKafka creates a franz-go producer and consumer.
func initKafka(brokers string, db *sql.DB, bridge *stovekafka.Bridge) (KafkaProducer, func(), error) {
	if brokers == "" {
		return nil, func() {}, nil
	}

	log.Printf("Kafka initializing with franz-go brokers=%s", brokers)
	return initFranzKafka(brokers, "go-showcase", db, bridge)
}

// handleProductUpdate applies a product update consumed from Kafka.
func handleProductUpdate(db *sql.DB, value []byte) (bool, error) {
	var event ProductUpdateEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Printf("failed to unmarshal update event: %v", err)
		return false, nil
	}

	updated, err := updateProduct(context.Background(), db, event.ID, event.Name, event.Price)
	if err != nil {
		log.Printf("failed to update product %s: %v", event.ID, err)
		return false, err
	}
	return updated, nil
}

func reportConsumed(
	ctx context.Context,
	bridge *stovekafka.Bridge,
	topic string,
	key string,
	value []byte,
	partition int32,
	offset int64,
	headers map[string]string,
) {
	if bridge == nil {
		return
	}
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_ = bridge.ReportConsumed(reportCtx, &stovekafka.ConsumedMessage{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Partition: partition,
		Offset:    offset,
		Headers:   headers,
	})
}

// kafkaHeaders retains Stove correlation and injects the active W3C context.
func kafkaHeaders(r *http.Request) map[string]string {
	headers := map[string]string{}
	if testCase := r.Header.Get("testCase"); testCase != "" {
		headers["testCase"] = testCase
	}
	otel.GetTextMapPropagator().Inject(r.Context(), propagation.MapCarrier(headers))
	return headers
}
