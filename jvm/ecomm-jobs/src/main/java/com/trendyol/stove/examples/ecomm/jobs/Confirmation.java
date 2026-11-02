package com.trendyol.stove.examples.ecomm.jobs;

import java.io.Serializable;
import java.util.Map;

/** The information needed by the asynchronous order confirmation. */
public record Confirmation(String orderId, String userId, String productId, double amount, Map<String, String> correlation)
    implements Serializable {}
