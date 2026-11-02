package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type orderCreated struct {
	OrderID   string    `json:"orderId"`
	UserID    string    `json:"userId"`
	ProductID string    `json:"productId"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"createdAt"`
}

// updateStatistics is idempotent: a replayed order cannot increase totals twice.
func updateStatistics(ctx context.Context, db *sql.DB, value []byte) (bool, error) {
	var event orderCreated
	if err := json.Unmarshal(value, &event); err != nil || event.OrderID == "" || event.UserID == "" {
		return false, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "INSERT INTO processed_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING", event.OrderID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if count > 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO user_order_statistics (user_id, total_orders, total_amount, last_order_at) VALUES ($1,1,$2,NOW()) ON CONFLICT (user_id) DO UPDATE SET total_orders = user_order_statistics.total_orders + 1, total_amount = user_order_statistics.total_amount + EXCLUDED.total_amount, last_order_at = NOW()`, event.UserID, event.Amount)
		if err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func completionEvent(topic string, db *sql.DB, value []byte) (string, []byte, error) {
	if topic == topicProductUpdate {
		updated, err := handleProductUpdate(db, value)
		if updated {
			return "product.updated", value, err
		}
		return "", nil, err
	}
	if topic == "showcase.orders.created" {
		updated, err := updateStatistics(context.Background(), db, value)
		if err != nil || !updated {
			return "", nil, err
		}
		var event orderCreated
		_ = json.Unmarshal(value, &event)
		completion, err := json.Marshal(map[string]string{"orderId": event.OrderID, "userId": event.UserID})
		return "customer.statistics.updated", completion, err
	}
	return "", nil, nil
}
