package com.trendyol.stove.examples.ecomm.java;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import java.util.function.Supplier;

final class ProductSpans {
  private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("ecomm-java");

  private ProductSpans() {}

  static <T> T inSpan(String name, Supplier<T> body) {
    var span = TRACER.spanBuilder(name).startSpan();
    try (var ignored = span.makeCurrent()) {
      return body.get();
    } finally {
      span.end();
    }
  }
}
