package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/stove-io/examples/go/ecomm/internal/fraud"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type checkoutRequest struct {
	UserID    string  `json:"userId"`
	ProductID string  `json:"productId"`
	Amount    float64 `json:"amount"`
}

type checkoutService struct {
	db                       *sql.DB
	producer                 KafkaProducer
	confirmations            *confirmationQueue
	fraud                    fraud.FraudDetectionServiceClient
	inventoryURL, paymentURL string
	http                     *http.Client
}

func newCheckoutService(db *sql.DB, producer KafkaProducer, confirmations *confirmationQueue) (*checkoutService, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(getEnv("FRAUD_GRPC_TARGET", "localhost:9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return &checkoutService{db: db, producer: producer, confirmations: confirmations, fraud: fraud.NewFraudDetectionServiceClient(conn), inventoryURL: getEnv("INVENTORY_URL", "http://localhost:8081"), paymentURL: getEnv("PAYMENT_URL", "http://localhost:8081"), http: &http.Client{Timeout: 5 * time.Second}}, conn, nil
}

func (s *checkoutService) create(w http.ResponseWriter, r *http.Request) {
	var request checkoutRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || strings.TrimSpace(request.UserID) == "" || strings.TrimSpace(request.ProductID) == "" {
		checkoutError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	id := uuid.NewString()
	check, err := s.fraud.CheckFraud(ctx, &fraud.CheckFraudRequest{OrderId: id, UserId: request.UserID, ProductId: request.ProductID, Amount: request.Amount})
	if err != nil || check.GetIsFraudulent() {
		checkoutError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Fraud check failed")
		return
	}
	var inventory struct {
		Available bool `json:"available"`
	}
	if err := s.request(ctx, http.MethodGet, s.inventoryURL+"/inventory/"+url.PathEscape(request.ProductID), nil, &inventory); err != nil {
		checkoutError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Inventory request failed")
		return
	}
	if !inventory.Available {
		checkoutError(w, http.StatusConflict, "INVENTORY_NOT_AVAILABLE", "Inventory not available")
		return
	}
	var payment struct {
		Success       bool   `json:"success"`
		TransactionID string `json:"transactionId"`
		ErrorMessage  string `json:"errorMessage"`
	}
	if err := s.request(ctx, http.MethodPost, s.paymentURL+"/payments/charge", request, &payment); err != nil {
		checkoutError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Payment request failed")
		return
	}
	if !payment.Success {
		checkoutError(w, http.StatusBadGateway, "PAYMENT_FAILED", "Payment failed: "+payment.ErrorMessage)
		return
	}
	order := streamedOrder{OrderID: id, UserID: request.UserID, ProductID: request.ProductID, Amount: request.Amount, Status: "CONFIRMED"}
	_, err = s.db.ExecContext(ctx, "INSERT INTO orders (id, user_id, product_id, amount, status, payment_transaction_id) VALUES ($1,$2,$3,$4,$5,$6)", id, request.UserID, request.ProductID, request.Amount, order.Status, payment.TransactionID)
	if err != nil {
		checkoutError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Order persistence failed")
		return
	}
	if err := s.confirmations.schedule(ctx, confirmationArgs{OrderID: id, UserID: request.UserID, ProductID: request.ProductID, Amount: request.Amount, Correlation: kafkaHeaders(r)}); err != nil {
		checkoutError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Confirmation scheduling failed")
		return
	}
	now := time.Now().UTC()
	if s.producer != nil {
		created, _ := json.Marshal(orderCreated{id, request.UserID, request.ProductID, request.Amount, now})
		paymentEvent, _ := json.Marshal(map[string]any{"orderId": id, "transactionId": payment.TransactionID, "amount": request.Amount, "success": true, "createdAt": now})
		if err := s.producer.SendMessage(ctx, "showcase.orders.created", id, created, kafkaHeaders(r)); err != nil {
			checkoutError(w, 500, "INTERNAL_ERROR", "Order publication failed")
			return
		}
		if err := s.producer.SendMessage(ctx, "showcase.payments.processed", id, paymentEvent, kafkaHeaders(r)); err != nil {
			checkoutError(w, 500, "INTERNAL_ERROR", "Payment publication failed")
			return
		}
	}
	writeJSON(w, http.StatusCreated, order)
}

func (s *checkoutService) get(w http.ResponseWriter, r *http.Request) {
	var order streamedOrder
	err := s.db.QueryRowContext(r.Context(), "SELECT id, user_id, product_id, amount, status FROM orders WHERE id = $1", r.PathValue("id")).Scan(&order.OrderID, &order.UserID, &order.ProductID, &order.Amount, &order.Status)
	if err == sql.ErrNoRows {
		checkoutError(w, 404, "NOT_FOUND", "Order not found")
		return
	}
	if err != nil {
		checkoutError(w, 500, "INTERNAL_ERROR", "Order lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (s *checkoutService) request(ctx context.Context, method, target string, body any, result any) error {
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upstream returned %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func checkoutError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"errorCode": code, "message": message})
}
