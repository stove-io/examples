package com.trendyol.stove.examples.ecomm.java;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestController
class ProductController {
  private final ProductRepository products;
  private final ProductMessaging messaging;

  ProductController(ProductRepository products, ProductMessaging messaging) {
    this.products = products;
    this.messaging = messaging;
  }

  @GetMapping("/health")
  Map<String, String> health() {
    return Map.of("status", "UP");
  }

  @PostMapping("/api/products")
  ResponseEntity<?> create(@RequestBody CreateProductRequest request) {
    return ProductSpans.inSpan("POST /api/products", () -> {
      if (request.name() == null || request.name().isBlank() || request.price() == null) {
        return ResponseEntity.badRequest().body(Map.of("error", "invalid request body"));
      }
      var product = products.insert(request.name(), request.price());
      messaging.publishCreated(product);
      return ResponseEntity.status(HttpStatus.CREATED).body(product);
    });
  }

  @GetMapping("/api/products/{id}")
  ResponseEntity<?> get(@PathVariable String id) {
    return ProductSpans.inSpan("GET /api/products/{id}", () -> products.find(id)
      .<ResponseEntity<?>>map(ResponseEntity::ok)
      .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not found"))));
  }

  @GetMapping("/api/products")
  ResponseEntity<?> list() {
    return ProductSpans.inSpan("GET /api/products", () -> ResponseEntity.ok(products.list()));
  }
}

@RestControllerAdvice
class ApiErrors {
  @ExceptionHandler(HttpMessageNotReadableException.class)
  ResponseEntity<Map<String, String>> unreadable() {
    return ResponseEntity.badRequest().body(Map.of("error", "invalid request body"));
  }
}
