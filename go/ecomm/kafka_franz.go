package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	stovekafka "github.com/trendyol/stove/go/stove-kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type franzProduceClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type franzProducer struct {
	client franzProduceClient
	bridge *stovekafka.Bridge
}

func (p *franzProducer) SendMessage(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	var kafkaHeaders []kgo.RecordHeader
	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	record := &kgo.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: kafkaHeaders,
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return err
	}
	reportCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.bridge.ReportPublished(reportCtx, &stovekafka.PublishedMessage{Topic: topic, Key: key, Value: value, Headers: headers})
	return nil
}

func (p *franzProducer) Close() error {
	p.client.Close()
	return nil
}

func initFranzKafka(brokers, groupID string, db *sql.DB, bridge *stovekafka.Bridge) (KafkaProducer, func(), error) {
	brokerList := strings.Split(brokers, ",")

	// Separate producer client — no consumer group overhead
	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, nil, err
	}

	// Separate consumer client — consumer group coordination won't block produces
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.ConsumeTopics(topicProductUpdate, "showcase.orders.created"),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		producerClient.Close()
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	producer := &franzProducer{client: producerClient, bridge: bridge}

	go func() {
		for {
			fetches := consumerClient.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			for _, r := range fetches.Records() {
				if r.Topic == topicProductUpdate || r.Topic == "showcase.orders.created" {
					reportConsumed(ctx, bridge, r.Topic, string(r.Key), r.Value, r.Partition, r.Offset, franzConsumedHeaders(r.Headers))
					completionTopic, completion, err := completionEvent(r.Topic, db, r.Value)
					if err != nil {
						return
					}
					if completionTopic != "" {
						if err := producer.SendMessage(ctx, completionTopic, string(r.Key), completion, franzConsumedHeaders(r.Headers)); err != nil {
							log.Printf("failed to publish product.updated: %v", err)
							return
						}
					}
					if err := consumerClient.CommitRecords(ctx, r); err != nil {
						log.Printf("failed to commit product.update: %v", err)
						return
					}
					reportCtx, reportCancel := context.WithTimeout(context.Background(), time.Second)
					_ = bridge.ReportCommitted(reportCtx, r.Topic, r.Partition, r.Offset+1)
					reportCancel()
				}
			}
		}
	}()

	stop := func() {
		cancel()
		consumerClient.Close()
		producerClient.Close()
	}

	log.Printf("Kafka (franz-go) initialized")
	return producer, stop, nil
}

func franzConsumedHeaders(headers []kgo.RecordHeader) map[string]string {
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
