package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

type streamedOrder struct {
	OrderID   string  `json:"orderId"`
	UserID    string  `json:"userId"`
	ProductID string  `json:"productId"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
}

func handleOrderStream(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.QueryContext(r.Context(), "SELECT id, user_id, product_id, amount, status FROM orders WHERE user_id = $1 ORDER BY created_at, id", r.URL.Query().Get("userId"))
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		// Read before writing headers so database errors are returned to the caller.
		var orders []streamedOrder
		for rows.Next() {
			var order streamedOrder
			if err := rows.Scan(&order.OrderID, &order.UserID, &order.ProductID, &order.Amount, &order.Status); err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			orders = append(orders, order)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		encoder := json.NewEncoder(w)
		for _, order := range orders {
			if err := encoder.Encode(order); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}
}
