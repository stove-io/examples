package com.trendyol.stove.examples.ecomm.ktor

import com.trendyol.stove.examples.ecomm.ktor.orders.OrderStreamRepository
import com.trendyol.stove.examples.ecomm.ktor.orders.startOrderQueryServer
import com.trendyol.stove.examples.ecomm.ktor.checkout.Checkout
import com.trendyol.stove.examples.ecomm.ktor.checkout.CheckoutRequest
import com.trendyol.stove.examples.ecomm.ktor.checkout.CheckoutFailure
import com.trendyol.stove.examples.ecomm.jobs.ConfirmationScheduler
import org.postgresql.ds.PGSimpleDataSource
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respondTextWriter
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.util.AttributeKey
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Tracer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

private val json: ObjectMapper = jacksonObjectMapper()
private val tracer: Tracer = GlobalOpenTelemetry.getTracer("ecomm-ktor")
val OrderRepositoryKey = AttributeKey<OrderStreamRepository>("orders")

fun main(args: Array<String>) {
  run(args, wait = true)
}

fun run(args: Array<String>, wait: Boolean, onServerStarted: (() -> Unit) -> Unit = {}, onSchedulerStarted: (ConfirmationScheduler) -> Unit = {}): Application {
  val config = parseArgs(args)
  val port = config["server.port"]?.toInt() ?: 8080
  val database = Database(config)
  val orders = OrderStreamRepository(config)
  val orderQueryServer = startOrderQueryServer(config)
  val datasource = PGSimpleDataSource().apply {
    setURL("jdbc:postgresql://${config["db.host"]}:${config["db.port"]}/${config["db.name"]}")
    user = config["db.username"]
    password = config["db.password"]
  }
  lateinit var messaging: Messaging
  val confirmations = ConfirmationScheduler(datasource) { confirmation ->
    messaging.publish("order.confirmation.sent", confirmation.orderId(), json.writeValueAsString(mapOf(
      "orderId" to confirmation.orderId(), "userId" to confirmation.userId(),
      "productId" to confirmation.productId(), "amount" to confirmation.amount()
    )), confirmation.correlation())
  }.also(onSchedulerStarted)
  val checkout = Checkout(config, confirmations) { topic, key, payload -> messaging.publish(topic, key, payload) }
  messaging = Messaging(config, database, checkout)
  val server = embeddedServer(Netty, port = port, host = "0.0.0.0") {
    attributes.put(OrderRepositoryKey, orders)
    install(ContentNegotiation) {
      jackson { registerModule(kotlinModule()) }
    }
    routing {
      get("/api/orders/stream") {
        val customerOrders = orders.findByUser(call.request.queryParameters["userId"].orEmpty())
        call.respondTextWriter(contentType = ContentType.parse("application/x-ndjson")) {
          customerOrders.forEach { order -> write(json.writeValueAsString(order)); write("\n"); flush() }
        }
      }
      post("/api/orders") {
        try {
          val request = call.receive<CheckoutRequest>()
          val correlation = listOf("traceparent", "tracestate", "baggage", "testcase").mapNotNull { name ->
            call.request.headers[name]?.let { (if (name == "testcase") "testCase" else name) to it }
          }.toMap()
          val order = withContext(Dispatchers.IO) { checkout.create(request, correlation) }
          call.respond(HttpStatusCode.Created, order)
        }
        catch (error: CheckoutFailure) { call.respond(HttpStatusCode.fromValue(error.status), mapOf("errorCode" to error.code, "message" to error.message)) }
      }
      get("/api/orders/{id}") {
        val id = call.parameters["id"].orEmpty()
        val order = withContext(Dispatchers.IO) { checkout.find(id) }
        if (order == null) call.respond(HttpStatusCode.NotFound, mapOf("errorCode" to "NOT_FOUND", "message" to "Order not found")) else call.respond(order)
      }
      get("/health") { call.respond(mapOf("status" to "UP")) }
      post("/api/products") {
        inSpan("POST /api/products") {
          val request = try {
            call.receive<CreateProductRequest>()
          } catch (_: Exception) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "invalid request body"))
            return@inSpan
          }
          val name = request.name
          val price = request.price
          if (name.isNullOrBlank() || price == null) {
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to "invalid request body"))
            return@inSpan
          }
          val product = database.insert(name, price)
          messaging.publishCreated(product)
          call.respond(HttpStatusCode.Created, product)
        }
      }
      get("/api/products/{id}") {
        inSpan("GET /api/products/{id}") {
          val product = database.find(call.parameters["id"].orEmpty())
          if (product == null) {
            call.respond(HttpStatusCode.NotFound, mapOf("error" to "not found"))
          } else {
            call.respond(product)
          }
        }
      }
      get("/api/products") {
        inSpan("GET /api/products") {
          call.respond(database.list())
        }
      }
    }
    monitor.subscribe(ApplicationStopped) { confirmations.close(); orderQueryServer.shutdown(); messaging.close(); checkout.close() }
  }
  server.start(wait = wait)
  onServerStarted { server.stop(1_000, 5_000) }
  return server.application
}

private fun parseArgs(args: Array<String>): Map<String, String> = args.mapNotNull { raw ->
  val text = raw.removePrefix("--")
  val separator = text.indexOf('=')
  if (separator <= 0) null else text.substring(0, separator) to text.substring(separator + 1)
}.toMap()

private suspend inline fun inSpan(name: String, crossinline block: suspend () -> Unit) {
  val span = tracer.spanBuilder(name).startSpan()
  val scope = span.makeCurrent()
  try {
    block()
  } finally {
    scope.close()
    span.end()
  }
}
