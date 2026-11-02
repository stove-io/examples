package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.examples.ecomm.ktor.run
import com.trendyol.stove.examples.ecomm.ktor.OrderRepositoryKey
import com.trendyol.stove.examples.ecomm.ktor.orders.OrderStreamRepository
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.ktor.ktor
import com.trendyol.stove.ktor.bridge
import com.trendyol.stove.system.PortFinder
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.using
import io.kotest.matchers.shouldBe
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension

private val APP_PORT = PortFinder.findAvailablePort()
private val ORDER_GRPC_PORT = PortFinder.findAvailablePort()
private val FRAUD_PORT = PortFinder.findAvailablePort()

class StoveConfig : AbstractProjectConfig() {
  private var stopApplication: (() -> Unit)? = null
  override val extensions: List<Extension> = listOf(StoveKotestExtension())

  override suspend fun beforeProject() {
    val schedulerProbe = SchedulerProbe()
    Stove()
      .with {
        sharedStove(appPort = APP_PORT, orderGrpcPort = ORDER_GRPC_PORT, appName = "ecomm-ktor", metadata = mapOf("language" to "kotlin", "framework" to "ktor", "runner" to "jvm"))

        sharedCheckoutDependencies(FRAUD_PORT)
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
          withParameters = listOf("server.port=$APP_PORT", "fraud.grpc.target=localhost:$FRAUD_PORT", "order.grpc.port=$ORDER_GRPC_PORT")
        )
      }.run()
  }

  override suspend fun afterProject() {
    stopApplication?.invoke()
    Stove.stop()
  }
}
