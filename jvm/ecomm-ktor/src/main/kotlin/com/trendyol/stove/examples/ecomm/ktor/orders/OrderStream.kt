package com.trendyol.stove.examples.ecomm.ktor.orders

import java.sql.DriverManager

data class StreamedOrder(val orderId: String, val userId: String, val productId: String, val amount: Double, val status: String)

class OrderStreamRepository(config: Map<String, String>) {
  private val url = "jdbc:postgresql://${config["db.host"]}:${config["db.port"]}/${config["db.name"]}"
  private val user = config["db.username"].orEmpty()
  private val password = config["db.password"].orEmpty()

  fun findByUser(userId: String): List<StreamedOrder> = DriverManager.getConnection(url, user, password).use { connection ->
    connection.prepareStatement("SELECT id, user_id, product_id, amount, status FROM orders WHERE user_id = ? ORDER BY created_at, id").use { statement ->
      statement.setString(1, userId)
      statement.executeQuery().use { rows ->
        buildList {
          while (rows.next()) add(StreamedOrder(rows.getString("id"), rows.getString("user_id"), rows.getString("product_id"), rows.getDouble("amount"), rows.getString("status")))
        }
      }
    }
  }
}
