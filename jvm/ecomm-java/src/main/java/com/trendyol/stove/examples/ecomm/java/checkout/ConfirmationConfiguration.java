package com.trendyol.stove.examples.ecomm.java.checkout;

import com.trendyol.stove.examples.ecomm.jobs.ConfirmationScheduler;
import javax.sql.DataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
class ConfirmationConfiguration {
  @Bean(destroyMethod = "close")
  ConfirmationScheduler confirmationScheduler(DataSource dataSource, KafkaTemplate<String, String> kafka, ObjectMapper json) {
    return new ConfirmationScheduler(dataSource, confirmation -> {
      try {
        var payload = Map.of("orderId", confirmation.orderId(), "userId", confirmation.userId(),
            "productId", confirmation.productId(), "amount", confirmation.amount());
        var record = new ProducerRecord<>("order.confirmation.sent", confirmation.orderId(), json.writeValueAsString(payload));
        confirmation.correlation().forEach((key, value) -> record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
        kafka.send(record).get();
      } catch (Exception error) {
        throw new IllegalStateException("Could not publish order confirmation", error);
      }
    });
  }
}
