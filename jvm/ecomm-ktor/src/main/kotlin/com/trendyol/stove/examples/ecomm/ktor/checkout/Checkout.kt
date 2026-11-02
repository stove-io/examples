package com.trendyol.stove.examples.ecomm.ktor.checkout

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.trendyol.stove.examples.kotlin.spring.grpc.CheckFraudRequest
import com.trendyol.stove.examples.kotlin.spring.grpc.FraudDetectionServiceGrpc
import com.trendyol.stove.examples.ecomm.jobs.Confirmation
import com.trendyol.stove.examples.ecomm.jobs.ConfirmationScheduler
import io.grpc.ManagedChannelBuilder
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.DriverManager
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

private val json = jacksonObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
data class CheckoutRequest(val userId: String, val productId: String, val amount: Double)
data class Order(val orderId: String, val userId: String, val productId: String, val amount: Double, val status: String)
data class OrderEvent(val orderId: String, val userId: String, val productId: String, val amount: Double, val createdAt: String? = null)
data class Inventory(val available: Boolean)
data class Payment(val success: Boolean, val transactionId: String? = null)
class CheckoutFailure(val status: Int, val code: String, override val message: String) : RuntimeException(message)

class Checkout(private val config: Map<String, String>, private val confirmations: ConfirmationScheduler, private val publish: (String, String, String) -> Unit) {
  private val fraudChannel = ManagedChannelBuilder.forTarget(config["fraud.grpc.target"] ?: "localhost:9090").usePlaintext().build()
  private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
  private fun connection() = DriverManager.getConnection("jdbc:postgresql://${config["db.host"]}:${config["db.port"]}/${config["db.name"]}", config["db.username"], config["db.password"])

  fun create(request: CheckoutRequest, correlation: Map<String, String>): Order {
    val id = UUID.randomUUID().toString()
    val fraud = FraudDetectionServiceGrpc.newBlockingStub(fraudChannel).withDeadlineAfter(5, TimeUnit.SECONDS)
      .checkFraud(CheckFraudRequest.newBuilder().setOrderId(id).setUserId(request.userId).setProductId(request.productId).setAmount(request.amount).build())
    if (fraud.isFraudulent) throw CheckoutFailure(500, "INTERNAL_ERROR", "Order flagged as fraudulent")
    val inventory = json.readValue<Inventory>(send("${config["external-apis.inventory.url"]}/inventory/${request.productId}"))
    if (!inventory.available) throw CheckoutFailure(409, "INVENTORY_NOT_AVAILABLE", "Inventory not available")
    val payment = json.readValue<Payment>(send("${config["external-apis.payment.url"]}/payments/charge", json.writeValueAsString(request)))
    if (!payment.success) throw CheckoutFailure(502, "PAYMENT_FAILED", "Payment failed")
    val transactionId = requireNotNull(payment.transactionId)
    val order = Order(id, request.userId, request.productId, request.amount, "CONFIRMED")
    connection().use { db -> db.prepareStatement("INSERT INTO orders (id,user_id,product_id,amount,status,payment_transaction_id) VALUES (?,?,?,?,?,?)").use { statement ->
      statement.setString(1, id); statement.setString(2, request.userId); statement.setString(3, request.productId)
      statement.setBigDecimal(4, request.amount.toBigDecimal()); statement.setString(5, order.status); statement.setString(6, transactionId); statement.executeUpdate()
    } }
    publish("showcase.orders.created", id, json.writeValueAsString(OrderEvent(id, request.userId, request.productId, request.amount)))
    publish("showcase.payments.processed", id, json.writeValueAsString(mapOf("orderId" to id, "transactionId" to transactionId, "amount" to request.amount, "success" to true)))
    confirmations.schedule(Confirmation(id, request.userId, request.productId, request.amount, correlation))
    return order
  }

  fun close() { fraudChannel.shutdownNow() }

  fun find(id: String): Order? = connection().use { db ->
    db.prepareStatement("SELECT id,user_id,product_id,amount,status FROM orders WHERE id=?").use { statement ->
      statement.setString(1, id); statement.executeQuery().use { rows ->
        if (!rows.next()) null else Order(rows.getString(1), rows.getString(2), rows.getString(3), rows.getDouble(4), rows.getString(5))
      }
    }
  }

  fun updateStatistics(payload: String): String? {
    val event = try { json.readValue<OrderEvent>(payload) } catch (_: Exception) { return null }
    connection().use { db -> db.prepareStatement("WITH inserted AS (INSERT INTO processed_orders (order_id) VALUES (?) ON CONFLICT DO NOTHING RETURNING order_id) INSERT INTO user_order_statistics (user_id,total_orders,total_amount,last_order_at) SELECT ?,1,?,NOW() FROM inserted ON CONFLICT (user_id) DO UPDATE SET total_orders=user_order_statistics.total_orders+1,total_amount=user_order_statistics.total_amount+EXCLUDED.total_amount,last_order_at=NOW()").use { statement ->
      statement.setString(1, event.orderId); statement.setString(2, event.userId); statement.setBigDecimal(3, event.amount.toBigDecimal()); statement.executeUpdate()
    } }
    return json.writeValueAsString(mapOf("orderId" to event.orderId, "userId" to event.userId))
  }

  private fun send(url: String, body: String? = null): String {
    val builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5))
    val request = if (body == null) builder.GET().build() else builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build()
    val response = http.send(request, HttpResponse.BodyHandlers.ofString())
    check(response.statusCode() in 200..299) { "Upstream request returned ${response.statusCode()}" }
    return response.body()
  }
}
