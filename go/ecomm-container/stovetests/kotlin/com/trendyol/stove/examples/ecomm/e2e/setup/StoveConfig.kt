package com.trendyol.stove.examples.ecomm.e2e.setup

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.Ports
import com.trendyol.stove.container.ContainerTarget
import com.trendyol.stove.container.containerApp
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.WithDsl
import com.trendyol.stove.system.application.EnvProvider
import org.testcontainers.Testcontainers
import org.testcontainers.containers.Network
import java.net.BindException
import java.net.ServerSocket
import java.net.URI
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
private val PORTS = SuitePorts(
  app = appPortReservation.localPort,
  orderGrpc = orderGrpcPortReservation.localPort,
  withOtlp = true
)
private val APP_IMAGE = requireNotNull(System.getProperty("go.app.container.image")?.takeIf { it.isNotBlank() }) {
  "go.app.container.image system property not set"
}
private const val COVERAGE_DIR_IN_CONTAINER = "/tmp/go-coverage"

class StoveConfig : EcommStoveConfig(
  ports = PORTS,
  appName = "ecomm-go",
  metadata = suiteMetadata(language = "go", runner = "container", extra = mapOf("kafka.library" to "franz-go")),
  profiling = goProfiling()
) {
  private val appNetwork = Network.newNetwork()

  override fun WithDsl.configureSuite() {
    val hostCoverageDir = System.getProperty("go.cover.dir").orEmpty()
    val coverageDirInContainer = if (hostCoverageDir.isBlank()) "" else COVERAGE_DIR_IN_CONTAINER
    val otlpPort = requireNotNull(ports.otlp)

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
      image = APP_IMAGE,
      target = ContainerTarget.Server(
        hostPort = ports.app,
        internalPort = ports.app,
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
          put("FRAUD_GRPC_TARGET", "host.testcontainers.internal:${ports.fraud}"); put("ORDER_GRPC_PORT", "${ports.orderGrpc}")
          put("OTEL_EXPORTER_OTLP_ENDPOINT", "host.testcontainers.internal:$otlpPort")
          put("STOVE_KAFKA_BRIDGE_HOST", "host.testcontainers.internal"); put("STOVE_KAFKA_BRIDGE_PORT", value("stove.kafka.bridge.port"))
          put("GOCOVERDIR", coverageDirInContainer)
          config["stove.profiling.serverUrl"]?.let { put("STOVE_PROFILING_SERVER_URL", hostUrl(it)) }
          listOf("runId" to "STOVE_RUN_ID", "serviceName" to "STOVE_PROFILING_SERVICE_NAME", "uploadIntervalSeconds" to "STOVE_PROFILING_INTERVAL_SECONDS", "profileTypes" to "STOVE_PROFILING_TYPES").forEach { (key, env) -> config["stove.profiling.$key"]?.let { put(env, it) } }
        }
      },
      beforeStarted = { config ->
        val exposedPorts = mutableSetOf(ports.fraud, otlpPort, config.getValue("stove.kafka.bridge.port").toInt(), URI(config.getValue("external-apis.inventory.url")).port)
        config["stove.profiling.serverUrl"]?.let { URI(it) }?.takeIf { it.host in listOf("localhost", "127.0.0.1") }?.let { exposedPorts.add(if (it.port == -1) 80 else it.port) }
        Testcontainers.exposeHostPorts(*exposedPorts.toIntArray())
        appPortReservation.close()
        orderGrpcPortReservation.close()
      },
      configureContainer = {
        withNetwork(appNetwork)
        withExposedPorts(ports.orderGrpc)
        withCreateContainerCmdModifier { command ->
          val hostConfig = command.hostConfig ?: HostConfig.newHostConfig()
          val bindings = hostConfig.portBindings ?: Ports()
          bindings.bind(ExposedPort(ports.orderGrpc), Ports.Binding.bindPort(ports.orderGrpc))
          command.withHostConfig(hostConfig.withPortBindings(bindings))
        }
        if (hostCoverageDir.isNotBlank()) {
          withFileSystemBind(hostCoverageDir, COVERAGE_DIR_IN_CONTAINER)
        }
      }
    )
  }

  override suspend fun afterProject() {
    try { Stove.stop() } finally {
      appPortReservation.close()
      orderGrpcPortReservation.close()
      appNetwork.close()
    }
  }
}
