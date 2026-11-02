package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.database.migrations.DatabaseMigration
import com.trendyol.stove.postgres.PostgresSqlMigrationContext

class ProductMigration : DatabaseMigration<PostgresSqlMigrationContext> {
  override val order: Int = 2

  override suspend fun execute(connection: PostgresSqlMigrationContext) {
    connection.operations.execute(
      """
      CREATE TABLE IF NOT EXISTS products (
          id VARCHAR(255) PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          price DECIMAL(10, 2) NOT NULL
      );
      CREATE TABLE IF NOT EXISTS scheduled_tasks (
          task_name TEXT NOT NULL,
          task_instance TEXT NOT NULL,
          task_data BYTEA,
          execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
          picked BOOLEAN NOT NULL,
          picked_by TEXT,
          last_success TIMESTAMP WITH TIME ZONE,
          last_failure TIMESTAMP WITH TIME ZONE,
          consecutive_failures INT,
          last_heartbeat TIMESTAMP WITH TIME ZONE,
          version BIGINT NOT NULL,
          priority SMALLINT,
          PRIMARY KEY (task_name, task_instance)
      );
      CREATE TABLE IF NOT EXISTS orders (
          id VARCHAR(255) PRIMARY KEY,
          user_id VARCHAR(255) NOT NULL,
          product_id VARCHAR(255) NOT NULL,
          amount DECIMAL(10, 2) NOT NULL,
          status VARCHAR(50) NOT NULL,
          payment_transaction_id VARCHAR(255),
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS processed_orders (order_id VARCHAR(255) PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS user_order_statistics (
          user_id VARCHAR(255) PRIMARY KEY,
          total_orders INT NOT NULL DEFAULT 0,
          total_amount DECIMAL(10, 2) NOT NULL DEFAULT 0,
          last_order_at TIMESTAMP
      );
      """.trimIndent()
    )
  }
}
