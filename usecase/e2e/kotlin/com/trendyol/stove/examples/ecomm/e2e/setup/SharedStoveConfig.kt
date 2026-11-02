package com.trendyol.stove.examples.ecomm.e2e.setup

import com.trendyol.stove.grpc.GrpcSystemOptions
import com.trendyol.stove.grpc.grpc
import com.trendyol.stove.dashboard.DashboardExternalProfilingOptions
import com.trendyol.stove.dashboard.DashboardIngestion
import com.trendyol.stove.dashboard.DashboardSystemOptions
import com.trendyol.stove.dashboard.dashboard
import com.trendyol.stove.http.HttpClientSystemOptions
import com.trendyol.stove.http.httpClient
import com.trendyol.stove.system.WithDsl
import com.trendyol.stove.examples.ecomm.e2e.tests.EcommKafkaSerde
import com.trendyol.stove.kafka.KafkaContainerOptions
import com.trendyol.stove.kafka.KafkaSystemOptions
import com.trendyol.stove.kafka.kafka
import com.trendyol.stove.postgres.PostgresqlOptions
import com.trendyol.stove.postgres.PostgresqlContainerOptions
import com.trendyol.stove.postgres.postgresql
import com.trendyol.stove.tracing.tracing
import com.trendyol.stove.testing.grpcmock.GrpcMockSystemOptions
import com.trendyol.stove.testing.grpcmock.grpcMock
import com.trendyol.stove.wiremock.WireMockSystemOptions
import com.trendyol.stove.wiremock.wiremock
import com.trendyol.stove.serialization.StoveSerde
import org.testcontainers.containers.Network

internal fun stoveServerUrl(): String = System.getenv("STOVE_SERVER_URL") ?: "http://localhost:4040"

/** Systems common to the same use cases, independent of the application runner. */
fun WithDsl.sharedStove(
  appPort: Int,
  orderGrpcPort: Int? = null,
  appName: String,
  metadata: Map<String, String>,
  otlpPort: Int? = null,
  profiling: DashboardExternalProfilingOptions? = null
) {
  httpClient { HttpClientSystemOptions(baseUrl = "http://localhost:$appPort") }
  if (orderGrpcPort != null) grpc { GrpcSystemOptions(host = "localhost", port = orderGrpcPort) }
  dashboard {
    val serverUrl = stoveServerUrl()
    DashboardSystemOptions(
      appName = appName,
      metadata = metadata + mapOf(
        "chapter" to (System.getProperty("stove.chapter") ?: "all"),
        "scenario" to (System.getProperty("stove.demo.failure") ?: "none"),
        "gitlab.project" to (System.getenv("CI_PROJECT_PATH") ?: "stove-io/examples")
      ),
      ingestion = DashboardIngestion.Http(serverUrl),
      externalProfiling = profiling
    )
  }
  tracing {
    if (otlpPort == null) enableSpanReceiver() else enableSpanReceiver(port = otlpPort)
  }
}

/** Each suite receives its own broker and database; only the property mapping is runner-specific. */
fun WithDsl.sharedInfrastructure(
  kafkaProperties: (com.trendyol.stove.kafka.KafkaExposedConfiguration) -> List<String>,
  databaseProperties: (com.trendyol.stove.rdbms.RelationalDatabaseExposedConfiguration) -> List<String>,
  riverJobs: Boolean = false,
  appNetwork: Network? = null
) {
  kafka {
    KafkaSystemOptions(
      serde = EcommKafkaSerde(),
      bridgeGrpcServerPort = 0,
      containerOptions = KafkaContainerOptions(tag = "8.0.3") {
        withStartupAttempts(3)
        if (appNetwork != null) { withNetwork(appNetwork); withListener("ecomm-kafka:19092") }
      },
      configureExposedConfiguration = kafkaProperties
    )
  }
  lateinit var database: com.trendyol.stove.rdbms.RelationalDatabaseExposedConfiguration
  // Stove closes systems in registration order, so remove River's trigger before PostgreSQL stops.
  if (riverJobs) river { database }
  postgresql {
    PostgresqlOptions(databaseName = "stove", container = PostgresqlContainerOptions {
      if (appNetwork != null) { withNetwork(appNetwork); withNetworkAliases("ecomm-postgres") }
    }, configureExposedConfiguration = { cfg -> database = cfg; databaseProperties(cfg) })
      .migrations { register<ProductMigration>() }
  }
}

/** Host-process applications use the same exposed infrastructure and River jobs. */
fun WithDsl.sharedProcessInfrastructure() {
  sharedInfrastructure(
    kafkaProperties = { cfg -> listOf("kafka.bootstrapServers=${cfg.bootstrapServers}") },
    databaseProperties = { cfg -> listOf(
      "database.host=${cfg.host}",
      "database.port=${cfg.port}",
      "database.name=stove",
      "database.username=${cfg.username}",
      "database.password=${cfg.password}"
    ) },
    riverJobs = true
  )
}

/** External checkout services are scoped to this Stove instance. */
fun WithDsl.sharedCheckoutDependencies(grpcPort: Int) {
  grpcMock { GrpcMockSystemOptions(port = grpcPort) }
  wiremock {
    WireMockSystemOptions(
      port = 0,
      serde = StoveSerde.jackson.anyByteArraySerde(),
      configureExposedConfiguration = { cfg ->
        listOf("external-apis.inventory.url=${cfg.baseUrl}", "external-apis.payment.url=${cfg.baseUrl}")
      }
    )
  }
}
