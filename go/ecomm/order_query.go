package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"

	"github.com/stove-io/examples/go/ecomm/internal/orderquery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const orderQueryColumns = `SELECT id, user_id, product_id, amount, status, payment_transaction_id, created_at FROM orders `

type orderQueryService struct {
	orderquery.UnimplementedOrderQueryServiceServer
	db *sql.DB
}

type orderScanner interface{ Scan(dest ...any) error }

func scanOrder(row orderScanner) (*orderquery.OrderProto, error) {
	order := &orderquery.OrderProto{}
	var transaction sql.NullString
	var createdAt sql.NullTime
	err := row.Scan(&order.Id, &order.UserId, &order.ProductId, &order.Amount, &order.Status, &transaction, &createdAt)
	if err != nil {
		return nil, err
	}
	if transaction.Valid {
		order.PaymentTransactionId = transaction.String
	}
	if createdAt.Valid {
		order.CreatedAt = createdAt.Time.UnixMilli()
	}
	return order, nil
}

func (s *orderQueryService) GetOrder(ctx context.Context, request *orderquery.GetOrderRequest) (*orderquery.GetOrderResponse, error) {
	order, err := scanOrder(s.db.QueryRowContext(ctx, orderQueryColumns+"WHERE id = $1", request.OrderId))
	if errors.Is(err, sql.ErrNoRows) {
		return &orderquery.GetOrderResponse{Found: false}, nil
	}
	if err != nil {
		log.Printf("order query: %v", err)
		return nil, status.Error(codes.Internal, "order query failed")
	}
	return &orderquery.GetOrderResponse{Found: true, Order: order}, nil
}

func (s *orderQueryService) GetOrdersByUser(ctx context.Context, request *orderquery.GetOrdersByUserRequest) (*orderquery.GetOrdersResponse, error) {
	rows, err := s.db.QueryContext(ctx, orderQueryColumns+"WHERE user_id = $1 ORDER BY created_at, id", request.UserId)
	if err != nil {
		log.Printf("order query: %v", err)
		return nil, status.Error(codes.Internal, "order query failed")
	}
	defer rows.Close()
	result := &orderquery.GetOrdersResponse{}
	for rows.Next() {
		order, err := scanOrder(rows)
		if err != nil {
			log.Printf("order query: %v", err)
			return nil, status.Error(codes.Internal, "order query failed")
		}
		result.Orders = append(result.Orders, order)
	}
	if err := rows.Err(); err != nil {
		log.Printf("order query: %v", err)
		return nil, status.Error(codes.Internal, "order query failed")
	}
	return result, nil
}

func startOrderQueryServer(db *sql.DB, port string) (*grpc.Server, error) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	orderquery.RegisterOrderQueryServiceServer(server, &orderQueryService{db: db})
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("order query server: %v", err)
		}
	}()
	return server, nil
}
