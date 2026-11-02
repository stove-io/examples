package com.trendyol.stove.examples.ecomm.ktor

data class CreateProductRequest(val name: String? = null, val price: Double? = null)

data class Product(val id: String, val name: String, val price: Double)

data class ProductEvent(val id: String, val name: String, val price: Double)
