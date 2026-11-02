package com.trendyol.stove.examples.ecomm.e2e.tests

import arrow.core.some
import com.trendyol.stove.examples.ecomm.e2e.setup.river
import com.trendyol.stove.examples.ecomm.e2e.setup.confirmations
import com.trendyol.stove.examples.ecomm.e2e.setup.shouldBeExecuted
import com.trendyol.stove.examples.ecomm.e2e.setup.RiverSystem
import com.trendyol.stove.grpc.grpc
import com.trendyol.stove.examples.kotlin.spring.grpc.GetOrderRequest
import com.trendyol.stove.examples.kotlin.spring.grpc.FraudDetectionServiceGrpc
import com.trendyol.stove.examples.kotlin.spring.grpc.GetOrdersByUserRequest
import com.trendyol.stove.examples.kotlin.spring.grpc.OrderQueryServiceGrpc
import com.trendyol.stove.http.http
import com.trendyol.stove.kafka.kafka
import com.trendyol.stove.postgres.postgresql
import com.trendyol.stove.system.stove
import com.trendyol.stove.tracing.tracing
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotliquery.Row
import java.util.UUID
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.toList
import com.google.protobuf.Empty
import com.trendyol.stove.testing.grpcmock.grpcMock
import com.trendyol.stove.wiremock.wiremock
import com.trendyol.stove.system.ValidationDsl
import com.github.tomakehurst.wiremock.http.RequestMethod

data class CreateProductRequest(
  val name: String,
  val price: Double
)

data class ProductResponse(
  val id: String,
  val name: String,
  val price: Double
)

data class ProductRow(
  val id: String,
  val name: String,
  val price: Double
)

data class ProductCreatedEvent(
  val id: String,
  val name: String,
  val price: Double
)

data class ProductUpdateEvent(
  val id: String,
  val name: String,
  val price: Double
)

private val productRowMapper: (Row) -> ProductRow = { row ->
  ProductRow(
    id = row.string("id"),
    name = row.string("name"),
    price = row.double("price")
  )
}

class ProductLifecycleTest :
  FunSpec({

    test("should create a product and verify via HTTP, database, and traces") {
      stove {
        val productName = "Catalog Product"
        val productPrice = 42.99
        var productId: String? = null

        http {
          postAndExpectBody<ProductResponse>(
            uri = "/api/products",
            body = CreateProductRequest(name = productName, price = productPrice).some()
          ) { actual ->
            actual.status shouldBe 201
            productId = actual.body().id
            UUID.fromString(actual.body().id).toString() shouldBe actual.body().id
            actual.body().name shouldBe productName
            actual.body().price shouldBe productPrice
          }
        }

        postgresql {
          shouldQuery<ProductRow>(
            query = "SELECT id, name, price FROM products WHERE id = '$productId'",
            mapper = productRowMapper
          ) { rows ->
            rows.size shouldBe 1
            rows.first().name shouldBe productName
            rows.first().price shouldBe productPrice
          }
        }

        http {
          getResponse<ProductResponse>(
            uri = "/api/products/$productId"
          ) { actual ->
            actual.status shouldBe 200
            actual.body().id shouldBe productId
            actual.body().name shouldBe productName
            actual.body().price shouldBe productPrice
          }
        }

        tracing {
          waitForSpans(2, 5000)
          shouldContainSpan("POST /api/products")
          shouldContainSpan("GET /api/products/{id}")
          shouldNotHaveFailedSpans()
        }
      }
    }

    test("should list all products") {
      stove {
        val created = mutableListOf<ProductResponse>()
        http {
          postAndExpectBody<ProductResponse>(
            uri = "/api/products",
            body = CreateProductRequest(name = "Product A", price = 10.0).some()
          ) { actual ->
            actual.body().name shouldBe "Product A"
            created.add(actual.body())
          }
        }

        http {
          postAndExpectBody<ProductResponse>(
            uri = "/api/products",
            body = CreateProductRequest(name = "Product B", price = 20.0).some()
          ) { actual ->
            actual.body().name shouldBe "Product B"
            created.add(actual.body())
          }
        }

        http {
          getMany<ProductResponse>(
            uri = "/api/products"
          ) { actual ->
            created.forEach { product -> actual.contains(product) shouldBe true }
          }
        }

        tracing {
          waitForSpans(2, 5000)
          shouldContainSpan("POST /api/products")
          shouldContainSpan("GET /api/products")
          shouldNotHaveFailedSpans()
        }
      }
    }

    test("should return 404 for non-existent product") {
      stove {
        http {
          getBodilessResponse("/api/products/non-existent-id") { actual ->
            actual.status shouldBe 404
          }
        }
      }
    }

    test("should reject malformed and incomplete products") {
      stove {
        val invalidBodies: List<Any> = listOf(
          "{",
          emptyMap<String, Any>(),
          mapOf("name" to "missing price"),
          mapOf("price" to 1.0),
          mapOf("name" to null, "price" to 1.0),
          mapOf("name" to "missing price", "price" to null),
          mapOf("name" to "   ", "price" to 1.0)
        )
        http {
          invalidBodies.forEach { body ->
            postAndExpectBodilessResponse("/api/products", body.some()) { it.status shouldBe 400 }
          }
        }
      }
    }

  })

class ProductEventsTest :
  FunSpec({

    test("should publish ProductCreatedEvent when product is created") {
      stove {
        var productId = ""
        http {
          postAndExpectBody<ProductResponse>(
            uri = "/api/products",
            body = CreateProductRequest(name = "Kafka Product", price = 29.99).some()
          ) { actual ->
            actual.status shouldBe 201
            actual.body().name shouldBe "Kafka Product"
            productId = actual.body().id
          }
        }

        kafka {
          shouldBePublished<ProductCreatedEvent>(10.seconds) {
            metadata.topic == "product.created" && actual.id == productId && actual.name == "Kafka Product" && actual.price == 29.99
          }

        }
      }
    }

    test("should consume product update events from Kafka") {
      stove {
        var productId: String? = null

        http {
          postAndExpectBody<ProductResponse>(
            uri = "/api/products",
            body = CreateProductRequest(name = "Original Name", price = 10.0).some()
          ) { actual ->
            actual.status shouldBe 201
            productId = actual.body().id
          }
        }

        kafka {
          publish("product.update", ProductUpdateEvent(id = productId!!, name = "Updated Name", price = 99.99))
          shouldBeConsumed<ProductUpdateEvent>(10.seconds) {
            metadata.topic == "product.update" && actual.id == productId && actual.name == "Updated Name" && actual.price == 99.99
          }
          // This event is emitted only after the database update completes.
          shouldBePublished<ProductUpdateEvent>(10.seconds) {
            metadata.topic == "product.updated" && actual.id == productId && actual.name == "Updated Name" && actual.price == 99.99
          }
        }

        postgresql {
          shouldQuery<ProductRow>(
            query = "SELECT id, name, price FROM products WHERE id = '$productId'",
            mapper = productRowMapper
          ) { rows ->
            rows.size shouldBe 1
            rows.first().name shouldBe "Updated Name"
            rows.first().price shouldBe 99.99
          }
        }
      }
    }

    test("commits unknown and malformed updates without publishing a successful update") {
      stove {
        val unknownId = UUID.randomUUID().toString()
        val marker = UUID.randomUUID().toString()
        kafka {
          publish("product.update", ProductUpdateEvent(unknownId, "Unknown", 1.0))
          shouldBeConsumed<ProductUpdateEvent>(10.seconds) {
            metadata.topic == "product.update" && actual.id == unknownId && actual.name == "Unknown" && actual.price == 1.0
          }
          val malformed = "{invalid-$marker".toByteArray()
          publishRaw("product.update", malformed, headers = mapOf("showcase" to marker))
          shouldBeConsumed<ByteArray>(10.seconds) {
            metadata.topic == "product.update" && actual.contentEquals(malformed) && metadata.headers["showcase"] == marker
          }
          // Receipt precedes processing. Wait for the broker-confirmed next offset before
          // checking the absence of business effects for the discarded record.
          val discarded = peekConsumedMessages(10.seconds, "product.update") { it.headers["showcase"] == marker }
          peekCommittedMessages(10.seconds, "product.update") {
            it.partition == discarded.partition && it.offset >= discarded.offset + 1
          }
          messageStore().publishedMessages().none {
            it.message.utf8().contains(unknownId) || it.message.utf8().contains(marker)
          } shouldBe true
        }
        postgresql {
          shouldQuery<ProductRow>(
            query = "SELECT id, name, price FROM products WHERE id = '$unknownId'",
            mapper = productRowMapper
          ) { rows -> rows.size shouldBe 0 }
        }
      }
    }
  })


data class StreamedOrder(
  val orderId: String,
  val userId: String,
  val productId: String,
  val amount: Double,
  val status: String
)

class SharedStreamingTest : FunSpec({
  test("stream only the customer's persisted orders") {
    stove {
      val userId = "stream-${UUID.randomUUID()}"
      val otherUserId = "stream-${UUID.randomUUID()}"
      val products = mutableListOf<ProductResponse>()
      http {
        listOf(CreateProductRequest("Book", 12.0), CreateProductRequest("Pen", 3.0)).forEach { request ->
          postAndExpectBody<ProductResponse>("/api/products", request.some()) {
            it.status shouldBe 201
            products.add(it.body())
          }
        }
      }
      val first = StreamedOrder(UUID.randomUUID().toString(), userId, products[0].id, products[0].price, "CONFIRMED")
      val second = StreamedOrder(UUID.randomUUID().toString(), userId, products[1].id, products[1].price, "CONFIRMED")
      val other = StreamedOrder(UUID.randomUUID().toString(), otherUserId, UUID.randomUUID().toString(), 9.0, "CONFIRMED")
      postgresql {
        listOf(first, second, other).forEach { order ->
          shouldExecute("INSERT INTO orders (id, user_id, product_id, amount, status) VALUES ('${order.orderId}', '${order.userId}', '${order.productId}', ${order.amount}, '${order.status}')")
        }
      }
      http {
        readJsonStream<StreamedOrder>("/api/orders/stream", queryParams = mapOf("userId" to userId)) { flow ->
          val results = withTimeout(10_000) { flow.toList() }
          results.toSet() shouldBe setOf(first, second)
          results.size shouldBe 2
        }
      }
    }
  }
})


data class CheckoutRequest(val userId: String, val productId: String, val amount: Double)
data class CheckoutResponse(val orderId: String, val userId: String, val productId: String, val amount: Double, val status: String)
data class CheckoutErrorResponse(val message: String, val errorCode: String)
data class CheckoutOrderEvent(val orderId: String, val userId: String, val productId: String, val amount: Double, val createdAt: Any? = null)
data class CheckoutPaymentEvent(val orderId: String, val transactionId: String, val amount: Double, val success: Boolean, val createdAt: Any? = null)
data class CheckoutStatisticsEvent(val orderId: String, val userId: String)
data class ConfirmationJob(val orderId: String, val userId: String, val productId: String, val amount: Double, val correlation: Map<String, String> = emptyMap())
data class CheckoutStatisticsRow(val userId: String, val totalOrders: Int, val totalAmount: Double)

private suspend fun ValidationDsl.configureCheckout(productId: String, available: Boolean, paymentSuccess: Boolean?) {
  // A default proto3 response has is_fraudulent=false and is wire-compatible with the fraud contract.
  grpcMock { mockUnary("frauddetection.FraudDetectionService", "CheckFraud", response = Empty.getDefaultInstance()) }
  wiremock {
    mockGet(url = "/inventory/$productId", statusCode = 200, responseBody = mapOf("productId" to productId, "available" to available, "quantity" to 1).some())
    if (paymentSuccess != null) mockPost(url = "/payments/charge", statusCode = 200,
      responseBody = mapOf("success" to paymentSuccess, "transactionId" to "txn-$productId", "amount" to 25.0, "errorMessage" to "declined").some())
  }
}

private suspend fun ValidationDsl.checkoutProduct(): ProductResponse {
  var product: ProductResponse? = null
  http { postAndExpectBody<ProductResponse>("/api/products", CreateProductRequest("Checkout product", 25.0).some()) { it.status shouldBe 201; product = it.body() } }
  return requireNotNull(product)
}

class SharedCheckoutTest : FunSpec({
  test("complete checkout and update customer statistics") {
    stove {
      val failureMode = System.getProperty("stove.demo.failure", "none")
      require(failureMode in setOf("none", "assertion", "persistence")) { "Unknown demo failure mode: $failureMode" }
      val userId = if (failureMode == "persistence") "demo-failure-${UUID.randomUUID()}" else "checkout-${UUID.randomUUID()}"
      if (failureMode == "persistence") {
        postgresql { shouldExecute("ALTER TABLE orders ADD CONSTRAINT demo_reject_user CHECK (user_id NOT LIKE 'demo-failure-%')") }
      }
      val product = checkoutProduct()
      configureCheckout(product.id, available = true, paymentSuccess = true)
      var order: CheckoutResponse? = null
      http {
        postAndExpectBody<CheckoutResponse>("/api/orders", CheckoutRequest(userId, product.id, product.price).some()) {
          it.status shouldBe 201
          order = it.body()
          UUID.fromString(it.body().orderId).toString() shouldBe it.body().orderId
          it.body() shouldBe CheckoutResponse(it.body().orderId, userId, product.id, product.price, if (failureMode == "assertion") "WRONG_STATUS" else "CONFIRMED")
        }
        getResponse<CheckoutResponse>("/api/orders/${requireNotNull(order).orderId}") { it.status shouldBe 200; it.body() shouldBe order }
      }
      wiremock {
        shouldHaveBeenCalled(method = RequestMethod.GET, url = "/inventory/${product.id}")
        shouldHaveBeenCalled(method = RequestMethod.POST, url = "/payments/charge", requestContaining = mapOf("userId" to userId))
      }
      val orderId = requireNotNull(order).orderId
      grpcMock {
        shouldHaveBeenCalled(FraudDetectionServiceGrpc.getCheckFraudMethod()) {
          it.orderId == orderId && it.userId == userId && it.productId == product.id && it.amount == product.price
        }
      }
      postgresql {
        shouldQuery<CheckoutResponse>("SELECT id, user_id, product_id, amount, status FROM orders WHERE id = '$orderId'", mapper = { row ->
          CheckoutResponse(row.string("id"), row.string("user_id"), row.string("product_id"), row.double("amount"), row.string("status"))
        }) { it.single() shouldBe order }
      }
      if (stove.getOrNone<RiverSystem>().isSome()) {
        river { shouldBeExecuted<ConfirmationJob>("order_confirmation") { it.orderId == orderId && it.userId == userId && it.productId == product.id && it.amount == product.price } }
      } else {
        confirmations { shouldBeExecuted(ConfirmationJob(orderId, userId, product.id, product.price)) }
        val system = stove.getOrNone<com.trendyol.stove.examples.ecomm.e2e.setup.ConfirmationSystem>().getOrNull()!!
        system.validateBean(this, orderId, userId)
      }
      grpc {
        rawChannel { channel ->
          val client = OrderQueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS)
          val response = client.getOrder(GetOrderRequest.newBuilder().setOrderId(orderId).build())
          response.found shouldBe true
          response.order.apply {
            id shouldBe orderId
            this.userId shouldBe userId
            productId shouldBe product.id
            amount shouldBe product.price
            status shouldBe "CONFIRMED"
            paymentTransactionId shouldBe "txn-${product.id}"
            (createdAt > 0) shouldBe true
          }
          val customerOrders = client.getOrdersByUser(GetOrdersByUserRequest.newBuilder().setUserId(userId).build())
          customerOrders.ordersList.map { it.id } shouldBe listOf(orderId)
          client.getOrder(GetOrderRequest.newBuilder().setOrderId(UUID.randomUUID().toString()).build()).found shouldBe false
        }
      }
      kafka {
        shouldBePublished<ConfirmationJob>(10.seconds) { metadata.topic == "order.confirmation.sent" && actual.orderId == orderId && actual.userId == userId && actual.productId == product.id && actual.amount == product.price }
        shouldBePublished<CheckoutOrderEvent>(10.seconds) { metadata.topic == "showcase.orders.created" && actual.orderId == orderId && actual.userId == userId && actual.productId == product.id && actual.amount == product.price }
        shouldBePublished<CheckoutPaymentEvent>(10.seconds) { metadata.topic == "showcase.payments.processed" && actual.orderId == orderId && actual.transactionId == "txn-${product.id}" && actual.amount == product.price && actual.success }
        shouldBeConsumed<CheckoutOrderEvent>(10.seconds) { metadata.topic == "showcase.orders.created" && actual.orderId == orderId && actual.userId == userId }
        shouldBePublished<CheckoutStatisticsEvent>(10.seconds) { metadata.topic == "customer.statistics.updated" && actual.orderId == orderId && actual.userId == userId }
      }
      postgresql {
        shouldQuery<CheckoutStatisticsRow>("SELECT user_id, total_orders, total_amount FROM user_order_statistics WHERE user_id = '$userId'", mapper = { row ->
          CheckoutStatisticsRow(row.string("user_id"), row.int("total_orders"), row.double("total_amount"))
        }) { it.single() shouldBe CheckoutStatisticsRow(userId, 1, product.price) }
      }
    }
  }

  listOf(false, true).forEach { declined ->
    test(if (declined) "declined payment leaves no order" else "unavailable inventory leaves no order") {
      stove {
        val userId = "failure-${UUID.randomUUID()}"
        val product = checkoutProduct()
        configureCheckout(product.id, available = declined, paymentSuccess = if (declined) false else null)
        http {
          postAndExpectBody<CheckoutErrorResponse>("/api/orders", CheckoutRequest(userId, product.id, product.price).some()) {
            it.status shouldBe if (declined) 502 else 409
            it.body().errorCode shouldBe if (declined) "PAYMENT_FAILED" else "INVENTORY_NOT_AVAILABLE"
          }
        }
        wiremock {
          if (declined) shouldHaveBeenCalled(method = RequestMethod.POST, url = "/payments/charge", requestContaining = mapOf("userId" to userId))
          else shouldNotHaveBeenCalled(method = RequestMethod.POST, url = "/payments/charge", requestContaining = mapOf("userId" to userId))
        }
        postgresql {
          shouldQuery<String>("SELECT id FROM orders WHERE user_id = '$userId'", mapper = { it.string("id") }) { it.size shouldBe 0 }
        }
        kafka { messageStore().publishedMessages().none { it.message.utf8().contains(userId) } shouldBe true }
      }
    }
  }
})
