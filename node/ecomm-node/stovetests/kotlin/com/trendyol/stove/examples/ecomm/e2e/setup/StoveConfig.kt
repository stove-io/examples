package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.process.ProcessApplicationOptions
import com.trendyol.stove.process.ProcessTarget
import com.trendyol.stove.process.envMapper
import com.trendyol.stove.process.processApp
import com.trendyol.stove.system.WithDsl
import java.io.File

private val PORTS = SuitePorts(withOtlp = true)
private val APP_SCRIPT = requireNotNull(System.getProperty("node.app.script")?.takeIf { it.isNotBlank() }) {
  "node.app.script system property not set"
}

class StoveConfig : EcommStoveConfig(
  ports = PORTS,
  appName = "ecomm-node",
  metadata = suiteMetadata(language = "typescript", runner = "process", framework = "node")
) {
  override fun WithDsl.configureSuite() {
    sharedProcessInfrastructure()

    processApp {
      ProcessApplicationOptions(
        command = listOf("node", APP_SCRIPT),
        workingDirectory = File(APP_SCRIPT).parentFile,
        target = ProcessTarget.Server(port = ports.app, portEnvVar = "APP_PORT"),
        envProvider = envMapper {
          commerceEnvironment(ports.fraud, ports.orderGrpc, requireNotNull(ports.otlp))
        }
      )
    }
  }
}
