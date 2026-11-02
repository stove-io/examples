package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.process.ProcessApplicationOptions
import com.trendyol.stove.process.ProcessTarget
import com.trendyol.stove.process.envMapper
import com.trendyol.stove.process.processApp
import com.trendyol.stove.system.WithDsl

private val PORTS = SuitePorts(withOtlp = true)
private val APP_BINARY = requireNotNull(System.getProperty("rust.app.binary")?.takeIf { it.isNotBlank() }) {
  "rust.app.binary system property not set"
}

class StoveConfig : EcommStoveConfig(
  ports = PORTS,
  appName = "ecomm-rust",
  metadata = suiteMetadata(language = "rust", runner = "process", framework = "axum")
) {
  override fun WithDsl.configureSuite() {
    sharedProcessInfrastructure()

    processApp {
      ProcessApplicationOptions(
        command = listOf(APP_BINARY),
        target = ProcessTarget.Server(port = ports.app, portEnvVar = "APP_PORT"),
        envProvider = envMapper {
          commerceEnvironment(ports.fraud, ports.orderGrpc, requireNotNull(ports.otlp))
        }
      )
    }
  }
}
