package com.trendyol.stove.examples.catalog.e2e.setup

import com.trendyol.stove.container.ContainerTarget
import com.trendyol.stove.container.containerApp
import com.trendyol.stove.dashboard.DashboardExternalProfilingOptions
import com.trendyol.stove.dashboard.DashboardIngestion
import com.trendyol.stove.dashboard.DashboardSystemOptions
import com.trendyol.stove.dashboard.dashboard
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.http.HttpClientSystemOptions
import com.trendyol.stove.http.httpClient
import com.trendyol.stove.kafka.KafkaSystemOptions
import com.trendyol.stove.kafka.kafka
import com.trendyol.stove.kafka.stoveKafkaBridgePortDefault
import com.trendyol.stove.postgres.PostgresqlOptions
import com.trendyol.stove.postgres.postgresql
import com.trendyol.stove.system.PortFinder
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.application.envMapper
import com.trendyol.stove.tracing.tracing
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension

private val APP_PORT = PortFinder.findAvailablePort()
private val OTLP_PORT = PortFinder.findAvailablePort()
private const val COVERAGE_DIR_IN_CONTAINER = "/tmp/go-coverage"

class StoveConfig : AbstractProjectConfig() {
  override val extensions: List<Extension> = listOf(StoveKotestExtension())

  override suspend fun beforeProject() {
    val appImage = System.getProperty("go.app.container.image").orEmpty()
    val kafkaLibrary = System.getProperty("kafka.library") ?: "sarama"
    val hostCoverageDir = System.getProperty("go.cover.dir").orEmpty()
    val coverageDirInContainer = if (hostCoverageDir.isBlank()) "" else COVERAGE_DIR_IN_CONTAINER
    require(appImage.isNotBlank()) { "go.app.container.image system property not set" }

    Stove()
      .with {
        httpClient {
          HttpClientSystemOptions(baseUrl = "http://localhost:$APP_PORT")
        }

        dashboard {
          val serverUrl = System.getenv("STOVE_SERVER_URL") ?: "http://localhost:4040"
          DashboardSystemOptions(
            appName = "catalog",
            metadata = mapOf(
              "language" to "go",
              "runner" to "container",
              "kafka.library" to kafkaLibrary,
              "gitlab.project" to (System.getenv("CI_PROJECT_PATH") ?: "stove-io/examples")
            ),
            ingestion = DashboardIngestion.Http(serverUrl),
            externalProfiling = DashboardExternalProfilingOptions(
              serverUrl = serverUrl,
              profileTypes = listOf("cpu", "alloc_space", "inuse_space", "goroutines")
            )
          )
        }

        tracing {
          enableSpanReceiver(port = OTLP_PORT)
        }

        kafka {
          KafkaSystemOptions(
            configureExposedConfiguration = { cfg ->
              listOf("kafka.bootstrapServers=${cfg.bootstrapServers}")
            }
          )
        }

        postgresql {
          PostgresqlOptions(
            databaseName = "stove",
            configureExposedConfiguration = { cfg ->
              listOf(
                "database.host=${cfg.host}",
                "database.port=${cfg.port}",
                "database.name=stove",
                "database.username=${cfg.username}",
                "database.password=${cfg.password}"
              )
            }
          ).migrations {
            register<ProductMigration>()
          }
        }

        containerApp(
          image = appImage,
          target = ContainerTarget.Server(
            hostPort = APP_PORT,
            internalPort = APP_PORT,
            portEnvVar = "APP_PORT",
            bindHostPort = false
          ),
          envProvider = envMapper {
            "database.host" to "DB_HOST"
            "database.port" to "DB_PORT"
            "database.name" to "DB_NAME"
            "database.username" to "DB_USER"
            "database.password" to "DB_PASS"
            "kafka.bootstrapServers" to "KAFKA_BROKERS"
            "stove.profiling.serverUrl" to "STOVE_PROFILING_SERVER_URL"
            "stove.profiling.runId" to "STOVE_RUN_ID"
            "stove.profiling.serviceName" to "STOVE_PROFILING_SERVICE_NAME"
            "stove.profiling.uploadIntervalSeconds" to "STOVE_PROFILING_INTERVAL_SECONDS"
            "stove.profiling.profileTypes" to "STOVE_PROFILING_TYPES"
            env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:$OTLP_PORT")
            env("KAFKA_LIBRARY", kafkaLibrary)
            env("STOVE_KAFKA_BRIDGE_PORT", stoveKafkaBridgePortDefault)
            env("GOCOVERDIR", coverageDirInContainer)
          },
          configureContainer = {
            withNetworkMode("host")
            if (hostCoverageDir.isNotBlank()) {
              withFileSystemBind(hostCoverageDir, COVERAGE_DIR_IN_CONTAINER)
            }
          }
        )
      }.run()
  }

  override suspend fun afterProject() {
    Stove.stop()
  }
}
