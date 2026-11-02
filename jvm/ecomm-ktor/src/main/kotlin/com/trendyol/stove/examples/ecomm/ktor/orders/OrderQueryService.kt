package com.trendyol.stove.examples.ecomm.ktor.orders

import com.trendyol.stove.examples.kotlin.spring.grpc.*
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.sql.DriverManager
import java.sql.ResultSet

class OrderQueryService(private val config: Map<String, String>) : OrderQueryServiceGrpc.OrderQueryServiceImplBase() {
  private fun query(where: String, value: String): List<OrderProto> {
    val url = "jdbc:postgresql://${config["db.host"]}:${config["db.port"]}/${config["db.name"]}"
    return DriverManager.getConnection(url, config["db.username"], config["db.password"]).use { db ->
      db.prepareStatement("SELECT * FROM orders $where").use { statement ->
        statement.setString(1, value)
        statement.executeQuery().use { rows -> buildList { while (rows.next()) add(toProto(rows)) } }
      }
    }
  }

  private fun toProto(row: ResultSet): OrderProto = OrderProto.newBuilder()
    .setId(row.getString("id")).setUserId(row.getString("user_id")).setProductId(row.getString("product_id"))
    .setAmount(row.getDouble("amount")).setStatus(row.getString("status"))
    .setPaymentTransactionId(row.getString("payment_transaction_id") ?: "")
    .setCreatedAt(row.getTimestamp("created_at").toInstant().toEpochMilli()).build()

  override fun getOrder(request: GetOrderRequest, observer: StreamObserver<GetOrderResponse>) {
    try {
      val order = query("WHERE id = ?", request.orderId).firstOrNull()
      val response = GetOrderResponse.newBuilder().setFound(order != null)
      if (order != null) response.order = order
      observer.onNext(response.build()); observer.onCompleted()
    } catch (error: Exception) { observer.onError(Status.INTERNAL.withDescription("Order query failed").withCause(error).asRuntimeException()) }
  }

  override fun getOrdersByUser(request: GetOrdersByUserRequest, observer: StreamObserver<GetOrdersResponse>) {
    try {
      val orders = query("WHERE user_id = ? ORDER BY created_at, id", request.userId)
      observer.onNext(GetOrdersResponse.newBuilder().addAllOrders(orders).build()); observer.onCompleted()
    } catch (error: Exception) { observer.onError(Status.INTERNAL.withDescription("Order query failed").withCause(error).asRuntimeException()) }
  }
}

fun startOrderQueryServer(config: Map<String, String>): Server =
  ServerBuilder.forPort(config["order.grpc.port"]?.toInt() ?: 9091).addService(OrderQueryService(config)).build().start()
