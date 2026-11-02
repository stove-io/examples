package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.examples.ecomm.java.EcommApplication
import com.trendyol.stove.examples.ecomm.jobs.ConfirmationScheduler
import com.trendyol.stove.examples.ecomm.java.orders.OrderQueryService
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.spring.springBoot
import com.trendyol.stove.spring.bridge
import com.trendyol.stove.system.PortFinder
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.using
import io.kotest.matchers.shouldBe
import org.springframework.context.ConfigurableApplicationContext
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension

private val APP_PORT = PortFinder.findAvailablePort()
private val ORDER_GRPC_PORT = PortFinder.findAvailablePort()
private val FRAUD_PORT = PortFinder.findAvailablePort()

class StoveConfig : AbstractProjectConfig() {
  private var application: ConfigurableApplicationContext? = null
  override val extensions: List<Extension> = listOf(StoveKotestExtension())

  override suspend fun beforeProject() {
    val schedulerProbe = SchedulerProbe()
    Stove()
      .with {
        sharedStove(appPort = APP_PORT, orderGrpcPort = ORDER_GRPC_PORT, appName = "ecomm-java", metadata = mapOf("language" to "java", "framework" to "spring-boot", "runner" to "jvm"))

        sharedCheckoutDependencies(FRAUD_PORT)
        bridge()
        confirmations(schedulerProbe::await, schedulerProbe::snapshot) { orderId, userId ->
          using<OrderQueryService> { belongsToUser(orderId, userId) shouldBe true }
        }

        sharedInfrastructure(
          kafkaProperties = { cfg -> listOf(
                "spring.kafka.bootstrap-servers=${cfg.bootstrapServers}",
                "spring.kafka.producer.properties.interceptor.classes=${cfg.interceptorClass}",
                "spring.kafka.consumer.properties.interceptor.classes=${cfg.interceptorClass}"
              ) },
          databaseProperties = { cfg -> listOf(
                "spring.datasource.url=${cfg.jdbcUrl}",
                "spring.datasource.username=${cfg.username}",
                "spring.datasource.password=${cfg.password}"
              ) }
        )

        springBoot(
          runner = { parameters -> EcommApplication.run(parameters).also { context ->
            application = context
            context.getBean(ConfirmationScheduler::class.java).addListener(schedulerProbe)
          } },
          withParameters = listOf("server.port=$APP_PORT", "fraud.grpc.target=localhost:$FRAUD_PORT", "order.grpc.port=$ORDER_GRPC_PORT")
        )
      }.run()
  }

  override suspend fun afterProject() {
    application?.close()
    Stove.stop()
  }
}
