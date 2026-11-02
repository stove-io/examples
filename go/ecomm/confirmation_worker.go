package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

type confirmationArgs struct {
	OrderID     string            `json:"orderId"`
	UserID      string            `json:"userId"`
	ProductID   string            `json:"productId"`
	Amount      float64           `json:"amount"`
	Correlation map[string]string `json:"correlation"`
}

func (confirmationArgs) Kind() string { return "order_confirmation" }

type confirmationWorker struct {
	river.WorkerDefaults[confirmationArgs]
	producer KafkaProducer
}

func (w *confirmationWorker) Work(ctx context.Context, job *river.Job[confirmationArgs]) error {
	args := job.Args
	log.Printf("Order confirmation for %s: %s", args.UserID, args.OrderID)
	if w.producer == nil {
		return nil
	}
	payload, err := json.Marshal(map[string]any{"orderId": args.OrderID, "userId": args.UserID, "productId": args.ProductID, "amount": args.Amount})
	if err != nil {
		return err
	}
	return w.producer.SendMessage(ctx, "order.confirmation.sent", args.OrderID, payload, args.Correlation)
}

type confirmationQueue struct {
	client *river.Client[pgx.Tx]
	pool   *pgxpool.Pool
}

func startConfirmationWorker(ctx context.Context, connStr string, producer KafkaProducer) (*confirmationQueue, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, err
	}
	driver := riverpgxv5.New(pool)
	migrator, err := rivermigrate.New(driver, nil)
	if err == nil {
		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &confirmationWorker{producer: producer})
	client, err := river.NewClient(driver, &river.Config{Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}}, Workers: workers})
	if err == nil {
		err = client.Start(ctx)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &confirmationQueue{client: client, pool: pool}, nil
}
func (q *confirmationQueue) schedule(ctx context.Context, args confirmationArgs) error {
	_, err := q.client.Insert(ctx, args, nil)
	return err
}
func (q *confirmationQueue) close(ctx context.Context) error {
	err := q.client.Stop(ctx)
	q.pool.Close()
	return err
}
