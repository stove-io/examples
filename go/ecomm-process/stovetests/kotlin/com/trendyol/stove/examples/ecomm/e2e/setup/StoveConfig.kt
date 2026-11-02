package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.dashboard.DashboardExternalProfilingOptions
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.process.ProcessTarget
import com.trendyol.stove.process.envMapper
import com.trendyol.stove.process.goApp
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
    val hostCoverageDir = System.getProperty("go.cover.dir").orEmpty()

    Stove()
      .with {
        sharedStove(appPort = APP_PORT, orderGrpcPort = ORDER_GRPC_PORT, appName = "ecomm-go", metadata = mapOf("language" to "go", "runner" to "process", "kafka.library" to "franz-go"), otlpPort = OTLP_PORT, profiling = DashboardExternalProfilingOptions(serverUrl = System.getenv("STOVE_SERVER_URL") ?: "http://localhost:4040", profileTypes = listOf("cpu", "alloc_space", "inuse_space", "goroutines")))

        sharedCheckoutDependencies(FRAUD_PORT)

        sharedProcessInfrastructure()

        goApp(
          target = ProcessTarget.Server(port = APP_PORT, portEnvVar = "APP_PORT"),
          envProvider = envMapper {
            commerceEnvironment(FRAUD_PORT, ORDER_GRPC_PORT, OTLP_PORT)
            "stove.profiling.serverUrl" to "STOVE_PROFILING_SERVER_URL"
            "stove.profiling.runId" to "STOVE_RUN_ID"
            "stove.profiling.serviceName" to "STOVE_PROFILING_SERVICE_NAME"
            "stove.profiling.uploadIntervalSeconds" to "STOVE_PROFILING_INTERVAL_SECONDS"
            "stove.profiling.profileTypes" to "STOVE_PROFILING_TYPES"
            env("GOCOVERDIR") {
              hostCoverageDir.takeIf { it.isNotBlank() }?.also { File(it).mkdirs() } ?: ""
            }
          }
        )
      }.run()
  }

  override suspend fun afterProject() {
    Stove.stop()
  }
}
