package com.trendyol.stove.examples.ecomm.java.checkout;

import com.trendyol.stove.examples.kotlin.spring.grpc.CheckFraudRequest;
import com.trendyol.stove.examples.kotlin.spring.grpc.FraudDetectionServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trendyol.stove.examples.ecomm.jobs.Confirmation;
import com.trendyol.stove.examples.ecomm.jobs.ConfirmationScheduler;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

@RestController
class CheckoutController {
  private final JdbcTemplate db;
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  private final ConfirmationScheduler confirmations;
  private final RestClient http = RestClient.create();
  private final String inventoryUrl, paymentUrl;
  private final ManagedChannel fraudChannel;

  CheckoutController(JdbcTemplate db, KafkaTemplate<String, String> kafka, ObjectMapper json, ConfirmationScheduler confirmations,
      @Value("${external-apis.inventory.url:http://localhost:8081}") String inventoryUrl,
      @Value("${external-apis.payment.url:http://localhost:8081}") String paymentUrl,
      @Value("${fraud.grpc.target:localhost:9090}") String fraudTarget) {
    this.db = db; this.kafka = kafka; this.json = json; this.confirmations = confirmations;
    this.inventoryUrl = inventoryUrl; this.paymentUrl = paymentUrl;
    this.fraudChannel = ManagedChannelBuilder.forTarget(fraudTarget).usePlaintext().build();
  }

  @PostMapping("/api/orders")
  ResponseEntity<?> checkout(@RequestBody CheckoutRequest request, @RequestHeader Map<String, String> headers) throws Exception {
    String id = UUID.randomUUID().toString();
    var fraud = FraudDetectionServiceGrpc.newBlockingStub(fraudChannel).withDeadlineAfter(5, TimeUnit.SECONDS)
      .checkFraud(CheckFraudRequest.newBuilder().setOrderId(id).setUserId(request.userId()).setProductId(request.productId()).setAmount(request.amount()).build());
    if (fraud.getIsFraudulent()) return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Order flagged as fraudulent");
    var inventory = http.get().uri(inventoryUrl + "/inventory/{id}", request.productId()).retrieve().body(Inventory.class);
    if (inventory == null || !inventory.available()) return error(HttpStatus.CONFLICT, "INVENTORY_NOT_AVAILABLE", "Inventory not available");
    var payment = http.post().uri(paymentUrl + "/payments/charge").body(request).retrieve().body(Payment.class);
    if (payment == null || !payment.success()) return error(HttpStatus.BAD_GATEWAY, "PAYMENT_FAILED", "Payment failed");
    var order = new Order(id, request.userId(), request.productId(), request.amount(), "CONFIRMED");
    db.update("INSERT INTO orders (id, user_id, product_id, amount, status, payment_transaction_id) VALUES (?,?,?,?,?,?)", id, request.userId(), request.productId(), request.amount(), order.status(), payment.transactionId());
    String createdAt = Instant.now().toString();
    kafka.send("showcase.orders.created", id, json.writeValueAsString(Map.of("orderId", id, "userId", request.userId(), "productId", request.productId(), "amount", request.amount(), "createdAt", createdAt))).get();
    kafka.send("showcase.payments.processed", id, json.writeValueAsString(Map.of("orderId", id, "transactionId", payment.transactionId(), "amount", request.amount(), "success", true, "createdAt", createdAt))).get();
    var correlation = new java.util.HashMap<String, String>();
    for (String name : new String[] {"traceparent", "tracestate", "baggage", "testcase"}) {
      if (headers.containsKey(name)) correlation.put(name.equals("testcase") ? "testCase" : name, headers.get(name));
    }
    confirmations.schedule(new Confirmation(id, request.userId(), request.productId(), request.amount(), correlation));
    return ResponseEntity.status(HttpStatus.CREATED).body(order);
  }

  @PreDestroy
  void close() { fraudChannel.shutdownNow(); }

  @GetMapping("/api/orders/{id}")
  ResponseEntity<?> get(@PathVariable String id) {
    return db.query("SELECT id, user_id, product_id, amount, status FROM orders WHERE id = ?", (rs, row) -> new Order(rs.getString(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getString(5)), id)
      .stream().findFirst().<ResponseEntity<?>>map(ResponseEntity::ok).orElseGet(() -> error(HttpStatus.NOT_FOUND, "NOT_FOUND", "Order not found"));
  }

  private ResponseEntity<?> error(HttpStatus status, String code, String message) {
    return ResponseEntity.status(status).body(Map.of("errorCode", code, "message", message));
  }
  record CheckoutRequest(String userId, String productId, double amount) {}
  record Order(String orderId, String userId, String productId, double amount, String status) {}
  record Inventory(boolean available) {}
  record Payment(boolean success, String transactionId) {}
}
