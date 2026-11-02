package com.trendyol.stove.examples.ecomm.java.checkout;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
class StatisticsListener {
  private final JdbcTemplate db;
  private final TransactionTemplate transaction;
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  StatisticsListener(JdbcTemplate db, TransactionTemplate transaction, KafkaTemplate<String, String> kafka, ObjectMapper json) {
    this.db = db; this.transaction = transaction; this.kafka = kafka; this.json = json;
  }
  @KafkaListener(topics = "showcase.orders.created", groupId = "ecomm-java")
  void onOrder(ConsumerRecord<String, String> record) throws Exception {
    var order = json.readValue(record.value(), OrderEvent.class);
    transaction.executeWithoutResult(status -> {
      int inserted = db.update("INSERT INTO processed_orders (order_id) VALUES (?) ON CONFLICT DO NOTHING", order.orderId());
      if (inserted > 0) db.update("INSERT INTO user_order_statistics (user_id, total_orders, total_amount, last_order_at) VALUES (?,1,?,NOW()) ON CONFLICT (user_id) DO UPDATE SET total_orders = user_order_statistics.total_orders + 1, total_amount = user_order_statistics.total_amount + EXCLUDED.total_amount, last_order_at = NOW()", order.userId(), order.amount());
    });
    kafka.send(new ProducerRecord<>("customer.statistics.updated", null, order.userId(), json.writeValueAsString(Map.of("orderId", order.orderId(), "userId", order.userId())), record.headers())).get();
  }
  record OrderEvent(String orderId, String userId, String productId, double amount, String createdAt) {}
}
