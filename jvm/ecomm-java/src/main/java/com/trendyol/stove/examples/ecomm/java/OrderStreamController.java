package com.trendyol.stove.examples.ecomm.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

record StreamedOrder(String orderId, String userId, String productId, double amount, String status) {}

@RestController
class OrderStreamController {
  private final JdbcTemplate jdbc;
  private final ObjectMapper json;

  OrderStreamController(JdbcTemplate jdbc, ObjectMapper json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  @GetMapping(value = "/api/orders/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
  StreamingResponseBody stream(@RequestParam String userId) {
    List<StreamedOrder> orders = jdbc.query(
      "SELECT id, user_id, product_id, amount, status FROM orders WHERE user_id = ? ORDER BY created_at, id",
      (row, index) -> new StreamedOrder(row.getString("id"), row.getString("user_id"), row.getString("product_id"), row.getDouble("amount"), row.getString("status")), userId);
    return output -> {
      for (var order : orders) {
        output.write(json.writeValueAsBytes(order));
        output.write('\n');
        output.flush();
      }
    };
  }
}
