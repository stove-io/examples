package main

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	stovekafka "github.com/trendyol/stove/go/stove-kafka"
	"github.com/trendyol/stove/go/stove-kafka/stoveobserver"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
)

type fakeProducer struct{ err error }

func (p fakeProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	return kgo.ProduceResults{{Record: records[0], Err: p.err}}
}
func (fakeProducer) Close() {}

type recordingObserver struct {
	stoveobserver.UnimplementedStoveKafkaObserverServiceServer
	published chan *stoveobserver.PublishedMessage
}

func (o *recordingObserver) OnPublishedMessage(_ context.Context, msg *stoveobserver.PublishedMessage) (*stoveobserver.Reply, error) {
	o.published <- msg
	return &stoveobserver.Reply{}, nil
}

func TestFranzProducerReportsOnlySuccessfulDelivery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	observer := &recordingObserver{published: make(chan *stoveobserver.PublishedMessage, 2)}
	stoveobserver.RegisterStoveKafkaObserverServiceServer(server, observer)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	t.Setenv("STOVE_KAFKA_BRIDGE_HOST", "127.0.0.1")
	bridge, err := stovekafka.NewBridge(strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bridge.Close() })

	producer := &franzProducer{client: fakeProducer{err: errors.New("broker failed")}, bridge: bridge}
	if err := producer.SendMessage(context.Background(), "orders", "key", []byte("payload"), nil); err == nil {
		t.Fatal("expected delivery failure")
	}
	select {
	case <-observer.published:
		t.Fatal("reported a failed delivery as published")
	default:
	}

	producer.client = fakeProducer{}
	if err := producer.SendMessage(context.Background(), "orders", "key", []byte("payload"), map[string]string{"testCase": "test-id"}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-observer.published:
		if msg.Topic != "orders" || msg.Key != "key" || string(msg.Message) != "payload" || msg.Headers["testCase"] != "test-id" {
			t.Fatalf("unexpected report: %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("successful delivery was not reported")
	}

	server.Stop()
	if err := producer.SendMessage(context.Background(), "orders", "key", []byte("payload"), nil); err != nil {
		t.Fatalf("observer outage changed delivery result: %v", err)
	}
}
