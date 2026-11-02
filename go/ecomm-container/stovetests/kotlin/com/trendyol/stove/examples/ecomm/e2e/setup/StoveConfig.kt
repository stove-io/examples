package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.container.ContainerTarget
import com.trendyol.stove.container.containerApp
import com.trendyol.stove.dashboard.DashboardExternalProfilingOptions
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.system.PortFinder
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.application.EnvProvider
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import org.testcontainers.Testcontainers
import org.testcontainers.containers.Network
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.Ports
import java.net.URI
import java.net.ServerSocket
import java.net.BindException
import kotlin.random.Random

// Docker Desktop assigns published ports independently of host socket reservations.
// Reserve below Docker's ephemeral range so dependency mappings cannot take these ports.
private fun reserveAppPort(): ServerSocket {
  repeat(100) {
    try { return ServerSocket(Random.nextInt(10000, 20000)) } catch (_: BindException) { }
  }
  error("Could not reserve a port for the container application")
}
private val appPortReservation = reserveAppPort()
private val orderGrpcPortReservation = reserveAppPort()
private val APP_PORT = appPortReservation.localPort
private val ORDER_GRPC_PORT = orderGrpcPortReservation.localPort
private val OTLP_PORT = PortFinder.findAvailablePort()
private val FRAUD_PORT = PortFinder.findAvailablePort()
private const val COVERAGE_DIR_IN_CONTAINER = "/tmp/go-coverage"

class StoveConfig : AbstractProjectConfig() {
  private val appNetwork = Network.newNetwork()
  override val extensions: List<Extension> = listOf(StoveKotestExtension())

  override suspend fun beforeProject() {
    val appImage = System.getProperty("go.app.container.image").orEmpty()
    val hostCoverageDir = System.getProperty("go.cover.dir").orEmpty()
    val coverageDirInContainer = if (hostCoverageDir.isBlank()) "" else COVERAGE_DIR_IN_CONTAINER
    require(appImage.isNotBlank()) { "go.app.container.image system property not set" }

    Stove()
      .with {
        sharedStove(appPort = APP_PORT, orderGrpcPort = ORDER_GRPC_PORT, appName = "ecomm-go", metadata = mapOf("language" to "go", "runner" to "container", "kafka.library" to "franz-go"), otlpPort = OTLP_PORT, profiling = DashboardExternalProfilingOptions(serverUrl = System.getenv("STOVE_SERVER_URL") ?: "http://localhost:4040", profileTypes = listOf("cpu", "alloc_space", "inuse_space", "goroutines")))

        sharedCheckoutDependencies(FRAUD_PORT)

        sharedInfrastructure(
          kafkaProperties = { listOf("kafka.bootstrapServers=ecomm-kafka:19092") },
          databaseProperties = { cfg -> listOf(
                "database.host=ecomm-postgres",
                "database.port=5432",
                "database.name=stove",
                "database.username=${cfg.username}",
                "database.password=${cfg.password}"
              ) },
          riverJobs = true,
          appNetwork = appNetwork
        )

        containerApp(
          image = appImage,
          target = ContainerTarget.Server(
            hostPort = APP_PORT,
            internalPort = APP_PORT,
            portEnvVar = "APP_PORT",
            bindHostPort = true
          ),
          envProvider = EnvProvider { config ->
            fun value(name: String) = config[name] ?: error("Missing application configuration: $name")
            fun hostUrl(url: String): String = URI(url).let { uri ->
              if (uri.host in listOf("localhost", "127.0.0.1")) URI(uri.scheme, uri.userInfo, "host.testcontainers.internal", uri.port, uri.path, uri.query, uri.fragment).toString() else url
            }
            buildMap {
              put("DB_HOST", value("database.host")); put("DB_PORT", value("database.port")); put("DB_NAME", value("database.name"))
              put("DB_USER", value("database.username")); put("DB_PASS", value("database.password"))
              put("KAFKA_BROKERS", value("kafka.bootstrapServers"))
              put("INVENTORY_URL", hostUrl(value("external-apis.inventory.url"))); put("PAYMENT_URL", hostUrl(value("external-apis.payment.url")))
              put("FRAUD_GRPC_TARGET", "host.testcontainers.internal:$FRAUD_PORT"); put("ORDER_GRPC_PORT", "$ORDER_GRPC_PORT")
              put("OTEL_EXPORTER_OTLP_ENDPOINT", "host.testcontainers.internal:$OTLP_PORT")
              put("STOVE_KAFKA_BRIDGE_HOST", "host.testcontainers.internal"); put("STOVE_KAFKA_BRIDGE_PORT", value("stove.kafka.bridge.port"))
              put("GOCOVERDIR", coverageDirInContainer)
              config["stove.profiling.serverUrl"]?.let { put("STOVE_PROFILING_SERVER_URL", hostUrl(it)) }
              listOf("runId" to "STOVE_RUN_ID", "serviceName" to "STOVE_PROFILING_SERVICE_NAME", "uploadIntervalSeconds" to "STOVE_PROFILING_INTERVAL_SECONDS", "profileTypes" to "STOVE_PROFILING_TYPES").forEach { (key, env) -> config["stove.profiling.$key"]?.let { put(env, it) } }
            }
          },
          beforeStarted = { config ->
            val ports = mutableSetOf(FRAUD_PORT, OTLP_PORT, config.getValue("stove.kafka.bridge.port").toInt(), URI(config.getValue("external-apis.inventory.url")).port)
            config["stove.profiling.serverUrl"]?.let { URI(it) }?.takeIf { it.host in listOf("localhost", "127.0.0.1") }?.let { ports.add(if (it.port == -1) 80 else it.port) }
            Testcontainers.exposeHostPorts(*ports.toIntArray())
            appPortReservation.close()
            orderGrpcPortReservation.close()
          },
          configureContainer = {
            withNetwork(appNetwork)
            withExposedPorts(ORDER_GRPC_PORT)
            withCreateContainerCmdModifier { command ->
              val hostConfig = command.hostConfig ?: HostConfig.newHostConfig()
              val bindings = hostConfig.portBindings ?: Ports()
              bindings.bind(ExposedPort(ORDER_GRPC_PORT), Ports.Binding.bindPort(ORDER_GRPC_PORT))
              command.withHostConfig(hostConfig.withPortBindings(bindings))
            }
            if (hostCoverageDir.isNotBlank()) {
              withFileSystemBind(hostCoverageDir, COVERAGE_DIR_IN_CONTAINER)
            }
          }
        )
      }.run()
  }

  override suspend fun afterProject() {
    try { Stove.stop() } finally {
      appPortReservation.close()
      orderGrpcPortReservation.close()
      appNetwork.close()
    }
  }
}
