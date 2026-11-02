package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.process.EnvMapperBuilder

/** Environment shared by the Go, Node.js, and Rust process applications. */
fun EnvMapperBuilder.commerceEnvironment(fraudPort: Int, orderGrpcPort: Int, otlpPort: Int) {
  "database.host" to "DB_HOST"
  "database.port" to "DB_PORT"
  "database.name" to "DB_NAME"
  "database.username" to "DB_USER"
  "database.password" to "DB_PASS"
  "kafka.bootstrapServers" to "KAFKA_BROKERS"
  "external-apis.inventory.url" to "INVENTORY_URL"
  "external-apis.payment.url" to "PAYMENT_URL"
  "stove.kafka.bridge.port" to "STOVE_KAFKA_BRIDGE_PORT"
  env("FRAUD_GRPC_TARGET", "localhost:$fraudPort")
  env("ORDER_GRPC_PORT", "$orderGrpcPort")
  env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:$otlpPort")
}
