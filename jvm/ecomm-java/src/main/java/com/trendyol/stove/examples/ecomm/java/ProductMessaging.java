package com.trendyol.stove.examples.ecomm.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

@Component
class ProductMessaging {
  static final String TOPIC_CREATED = "product.created";
  static final String TOPIC_UPDATE = "product.update";

  private static final Logger log = LoggerFactory.getLogger(ProductMessaging.class);

  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  private final ProductRepository products;

  ProductMessaging(KafkaTemplate<String, String> kafka, ObjectMapper json, ProductRepository products) {
    this.kafka = kafka;
    this.json = json;
    this.products = products;
  }

  void publishCreated(Product product) {
    try {
      var payload = json.writeValueAsString(new ProductEvent(product.id(), product.name(), product.price()));
      kafka.send(TOPIC_CREATED, product.id(), payload).get();
    } catch (Exception error) {
      log.warn("failed to publish product.created for {}", product.id(), error);
    }
  }

  @KafkaListener(topics = TOPIC_UPDATE, groupId = "ecomm-java")
  void onUpdate(ConsumerRecord<String, String> record) throws Exception {
    ProductEvent event;
    try {
      event = json.readValue(record.value(), ProductEvent.class);
    } catch (Exception error) {
      log.warn("ignored product.update payload", error);
      return;
    }
    if (products.update(event.id(), event.name(), event.price()) > 0) {
      var updated = new ProducerRecord<String, String>("product.updated", null, event.id(), record.value(), record.headers());
      kafka.send(updated).get();
    }
  }
}
