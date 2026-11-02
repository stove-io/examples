package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.process.ProcessTarget
import com.trendyol.stove.process.envMapper
import com.trendyol.stove.process.goApp
import com.trendyol.stove.system.WithDsl
import java.io.File

private val PORTS = SuitePorts(withOtlp = true)

class StoveConfig : EcommStoveConfig(
  ports = PORTS,
  appName = "ecomm-go",
  metadata = suiteMetadata(language = "go", runner = "process", extra = mapOf("kafka.library" to "franz-go")),
  profiling = goProfiling()
) {
  override fun WithDsl.configureSuite() {
    val hostCoverageDir = System.getProperty("go.cover.dir").orEmpty()

    sharedProcessInfrastructure()

    goApp(
      target = ProcessTarget.Server(port = ports.app, portEnvVar = "APP_PORT"),
      envProvider = envMapper {
        commerceEnvironment(ports.fraud, ports.orderGrpc, requireNotNull(ports.otlp))
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
  }
}
