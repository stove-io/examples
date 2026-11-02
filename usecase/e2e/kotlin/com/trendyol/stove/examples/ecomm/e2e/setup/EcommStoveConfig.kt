package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.dashboard.DashboardExternalProfilingOptions
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.system.PortFinder
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.WithDsl
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension

/** Ports reserved once per suite and shared by Stove systems and the application runner. */
class SuitePorts(
  val app: Int = PortFinder.findAvailablePort(),
  val orderGrpc: Int = PortFinder.findAvailablePort(),
  val fraud: Int = PortFinder.findAvailablePort(),
  withOtlp: Boolean = false
) {
  val otlp: Int? = if (withOtlp) PortFinder.findAvailablePort() else null
}

/** Dashboard metadata, kept consistent across suites. */
fun suiteMetadata(
  language: String,
  runner: String,
  framework: String? = null,
  extra: Map<String, String> = emptyMap()
): Map<String, String> = buildMap {
  put("language", language)
  if (framework != null) put("framework", framework)
  put("runner", runner)
  putAll(extra)
}

/** Profiling options for the Go applications. */
fun goProfiling(): DashboardExternalProfilingOptions = DashboardExternalProfilingOptions(
  serverUrl = stoveServerUrl(),
  profileTypes = listOf("cpu", "alloc_space", "inuse_space", "goroutines")
)

/**
 * Lifecycle and systems every commerce suite shares. A suite only describes its runner, the
 * runner-specific configuration mapping, and any resources it must release.
 */
abstract class EcommStoveConfig(
  protected val ports: SuitePorts,
  private val appName: String,
  private val metadata: Map<String, String>,
  private val profiling: DashboardExternalProfilingOptions? = null
) : AbstractProjectConfig() {
  final override val extensions: List<Extension> = listOf(StoveKotestExtension())

  final override suspend fun beforeProject() {
    Stove()
      .with {
        sharedStove(
          appPort = ports.app,
          orderGrpcPort = ports.orderGrpc,
          appName = appName,
          metadata = metadata,
          otlpPort = ports.otlp,
          profiling = profiling
        )
        sharedCheckoutDependencies(ports.fraud)
        configureSuite()
      }
      .run()
  }

  /** Registers the suite's infrastructure mapping and application runner. */
  protected abstract fun WithDsl.configureSuite()

  override suspend fun afterProject() {
    Stove.stop()
  }
}
