package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.examples.ecomm.ktor.OrderRepositoryKey
import com.trendyol.stove.examples.ecomm.ktor.orders.OrderStreamRepository
import com.trendyol.stove.examples.ecomm.ktor.run
import com.trendyol.stove.ktor.bridge
import com.trendyol.stove.ktor.ktor
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.WithDsl
import com.trendyol.stove.system.using
import io.kotest.matchers.shouldBe

private val PORTS = SuitePorts()

class StoveConfig : EcommStoveConfig(
  ports = PORTS,
  appName = "ecomm-ktor",
  metadata = suiteMetadata(language = "kotlin", runner = "jvm", framework = "ktor")
) {
  private var stopApplication: (() -> Unit)? = null
  private val schedulerProbe = SchedulerProbe()

  override fun WithDsl.configureSuite() {
    bridge { app, type ->
      require(type.classifier == OrderStreamRepository::class) { "Unsupported dependency: $type" }
      app.attributes[OrderRepositoryKey]
    }
    confirmations(schedulerProbe::await, schedulerProbe::snapshot) { orderId, userId ->
      using<OrderStreamRepository> { findByUser(userId).map { it.orderId } shouldBe listOf(orderId) }
    }

    sharedInfrastructure(
      kafkaProperties = { cfg -> listOf(
            "kafka.bootstrapServers=${cfg.bootstrapServers}",
            "kafka.interceptorClasses=${cfg.interceptorClass}"
          ) },
      databaseProperties = { cfg -> listOf(
            "db.host=${cfg.host}",
            "db.port=${cfg.port}",
            "db.name=stove",
            "db.username=${cfg.username}",
            "db.password=${cfg.password}"
          ) }
    )

    ktor(
      runner = { parameters -> run(parameters, wait = false, onServerStarted = { stopApplication = it }, onSchedulerStarted = { it.addListener(schedulerProbe) }) },
      withParameters = listOf("server.port=${ports.app}", "fraud.grpc.target=localhost:${ports.fraud}", "order.grpc.port=${ports.orderGrpc}")
    )
  }

  override suspend fun afterProject() {
    stopApplication?.invoke()
    Stove.stop()
  }
}
