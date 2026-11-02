package com.trendyol.stove.examples.ecomm.java;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
class ProductRepository {
  private static final RowMapper<Product> ROW = (row, index) -> new Product(
    row.getString("id"),
    row.getString("name"),
    row.getDouble("price")
  );

  private final JdbcTemplate jdbc;

  ProductRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  Product insert(String name, double price) {
    var product = new Product(UUID.randomUUID().toString(), name, price);
    jdbc.update(
      "INSERT INTO products (id, name, price) VALUES (?, ?, ?)",
      product.id(),
      product.name(),
      new BigDecimal(Double.toString(price))
    );
    return product;
  }

  Optional<Product> find(String id) {
    var rows = jdbc.query("SELECT id, name, price FROM products WHERE id = ?", ROW, id);
    return rows.stream().findFirst();
  }

  int update(String id, String name, double price) {
    return jdbc.update(
      "UPDATE products SET name = ?, price = ? WHERE id = ?",
      name,
      new BigDecimal(Double.toString(price)),
      id
    );
  }

  List<Product> list() {
    return jdbc.query("SELECT id, name, price FROM products", ROW);
  }
}
