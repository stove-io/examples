package com.trendyol.stove.examples.ecomm.java.orders;

import com.trendyol.stove.examples.kotlin.spring.grpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderQueryService extends OrderQueryServiceGrpc.OrderQueryServiceImplBase {
  private final JdbcTemplate db;
  public OrderQueryService(JdbcTemplate db) { this.db = db; }

  public boolean belongsToUser(String orderId, String userId) {
    return Boolean.TRUE.equals(db.queryForObject("SELECT EXISTS (SELECT 1 FROM orders WHERE id = ? AND user_id = ?)", Boolean.class, orderId, userId));
  }

  private OrderProto order(ResultSet row, int ignored) throws SQLException {
    var transaction = row.getString("payment_transaction_id");
    return OrderProto.newBuilder().setId(row.getString("id")).setUserId(row.getString("user_id"))
      .setProductId(row.getString("product_id")).setAmount(row.getDouble("amount"))
      .setStatus(row.getString("status")).setPaymentTransactionId(transaction == null ? "" : transaction)
      .setCreatedAt(row.getTimestamp("created_at").toInstant().toEpochMilli()).build();
  }

  @Override public void getOrder(GetOrderRequest request, StreamObserver<GetOrderResponse> observer) {
    try {
      var orders = db.query("SELECT * FROM orders WHERE id = ?", this::order, request.getOrderId());
      var response = GetOrderResponse.newBuilder().setFound(!orders.isEmpty());
      if (!orders.isEmpty()) response.setOrder(orders.getFirst());
      observer.onNext(response.build());
      observer.onCompleted();
    } catch (Exception error) { observer.onError(Status.INTERNAL.withDescription("Order query failed").withCause(error).asRuntimeException()); }
  }

  @Override public void getOrdersByUser(GetOrdersByUserRequest request, StreamObserver<GetOrdersResponse> observer) {
    try {
      var orders = db.query("SELECT * FROM orders WHERE user_id = ? ORDER BY created_at, id", this::order, request.getUserId());
      observer.onNext(GetOrdersResponse.newBuilder().addAllOrders(orders).build());
      observer.onCompleted();
    } catch (Exception error) { observer.onError(Status.INTERNAL.withDescription("Order query failed").withCause(error).asRuntimeException()); }
  }
}
