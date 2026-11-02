package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.process.ProcessApplicationOptions
import com.trendyol.stove.process.ProcessTarget
import com.trendyol.stove.process.envMapper
import com.trendyol.stove.process.processApp
import com.trendyol.stove.system.PortFinder
import com.trendyol.stove.system.Stove
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import java.io.File

private val APP_PORT = PortFinder.findAvailablePort()
private val ORDER_GRPC_PORT = PortFinder.findAvailablePort()
private val OTLP_PORT = PortFinder.findAvailablePort()
private val FRAUD_PORT = PortFinder.findAvailablePort()

class StoveConfig : AbstractProjectConfig() {
  override val extensions: List<Extension> = listOf(StoveKotestExtension())

  override suspend fun beforeProject() {
    val script = System.getProperty("node.app.script").orEmpty()
    require(script.isNotBlank()) { "node.app.script system property not set" }

    Stove()
      .with {
        sharedStove(appPort = APP_PORT, orderGrpcPort = ORDER_GRPC_PORT, appName = "ecomm-node", metadata = mapOf("language" to "typescript", "framework" to "node", "runner" to "process"), otlpPort = OTLP_PORT)

        sharedCheckoutDependencies(FRAUD_PORT)

        sharedProcessInfrastructure()

        processApp {
          ProcessApplicationOptions(
            command = listOf("node", script),
            workingDirectory = File(script).parentFile,
            target = ProcessTarget.Server(port = APP_PORT, portEnvVar = "APP_PORT"),
            envProvider = envMapper {
              commerceEnvironment(FRAUD_PORT, ORDER_GRPC_PORT, OTLP_PORT)
            }
          )
        }
      }.run()
  }

  override suspend fun afterProject() {
    Stove.stop()
  }
}
