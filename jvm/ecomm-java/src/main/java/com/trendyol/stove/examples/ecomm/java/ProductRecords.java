package com.trendyol.stove.examples.ecomm.java;

record CreateProductRequest(String name, Double price) {}

record Product(String id, String name, double price) {}

record ProductEvent(String id, String name, double price) {}
