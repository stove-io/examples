package com.trendyol.stove.examples.ecomm.ktor

import java.math.BigDecimal
import java.sql.DriverManager
import java.util.UUID

internal class Database(config: Map<String, String>) {
  private val url = "jdbc:postgresql://${config["db.host"]}:${config["db.port"]}/${config["db.name"]}"
  private val user = config["db.username"].orEmpty()
  private val password = config["db.password"].orEmpty()

  fun insert(name: String, price: Double): Product {
    val product = Product(UUID.randomUUID().toString(), name, price)
    connection().use { connection ->
      connection.prepareStatement("INSERT INTO products (id, name, price) VALUES (?, ?, ?)").use { statement ->
        statement.setString(1, product.id)
        statement.setString(2, product.name)
        statement.setBigDecimal(3, BigDecimal(price.toString()))
        statement.executeUpdate()
      }
    }
    return product
  }

  fun find(id: String): Product? = connection().use { connection ->
    connection.prepareStatement("SELECT id, name, price FROM products WHERE id = ?").use { statement ->
      statement.setString(1, id)
      statement.executeQuery().use { rows ->
        if (!rows.next()) return null
        Product(rows.getString("id"), rows.getString("name"), rows.getDouble("price"))
      }
    }
  }

  fun update(id: String, name: String, price: Double): Boolean =
    connection().use { connection ->
      connection.prepareStatement("UPDATE products SET name = ?, price = ? WHERE id = ?").use { statement ->
        statement.setString(1, name)
        statement.setBigDecimal(2, BigDecimal(price.toString()))
        statement.setString(3, id)
        statement.executeUpdate() > 0
      }
    }

  fun list(): List<Product> = connection().use { connection ->
    connection.prepareStatement("SELECT id, name, price FROM products").use { statement ->
      statement.executeQuery().use { rows ->
        buildList {
          while (rows.next()) {
            add(Product(rows.getString("id"), rows.getString("name"), rows.getDouble("price")))
          }
        }
      }
    }
  }

  private fun connection() = DriverManager.getConnection(url, user, password)
}
