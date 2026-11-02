package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.examples.ecomm.java.EcommApplication
import com.trendyol.stove.examples.ecomm.java.orders.OrderQueryService
import com.trendyol.stove.examples.ecomm.jobs.ConfirmationScheduler
import com.trendyol.stove.spring.bridge
import com.trendyol.stove.spring.springBoot
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.WithDsl
import com.trendyol.stove.system.using
import io.kotest.matchers.shouldBe
import org.springframework.context.ConfigurableApplicationContext

private val PORTS = SuitePorts()

class StoveConfig : EcommStoveConfig(
  ports = PORTS,
  appName = "ecomm-java",
  metadata = suiteMetadata(language = "java", runner = "jvm", framework = "spring-boot")
) {
  private var application: ConfigurableApplicationContext? = null
  private val schedulerProbe = SchedulerProbe()

  override fun WithDsl.configureSuite() {
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
      withParameters = listOf("server.port=${ports.app}", "fraud.grpc.target=localhost:${ports.fraud}", "order.grpc.port=${ports.orderGrpc}")
    )
  }

  override suspend fun afterProject() {
    application?.close()
    Stove.stop()
  }
}
