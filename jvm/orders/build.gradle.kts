plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.spring)
  alias(libs.plugins.spring.boot)
  alias(libs.plugins.protobuf)
  alias(libs.plugins.stove.tracing)
  id("com.trendyol.stove.e2e")
}

stoveE2e {
  sourceSetName.set("test-e2e")
}

dependencies {
  implementation(libs.spring.boot.webflux)
  implementation(libs.spring.boot.autoconfigure)
  implementation(libs.spring.boot.data.r2dbc)
  implementation(libs.spring.kafka)
  implementation(libs.postgresql.r2dbc)
  implementation(libs.r2dbc.pool)
  implementation(libs.spring.boot.jdbc)
  implementation(libs.postgresql)
  implementation(libs.db.scheduler)
  implementation(libs.kotlinx.core)
  implementation(libs.kotlinx.reactive)
  implementation(libs.kotlinx.reactor)
  implementation(libs.kotlinx.jdk8)
  implementation(libs.kotlin.logging)
  implementation(libs.opentelemetry.extension.kotlin)
  implementation(libs.opentelemetry.instrumentation.annotations)
  implementation(libs.grpc.protobuf)
  implementation(libs.grpc.stub)
  implementation(libs.grpc.netty)
  implementation(libs.grpc.kotlin.stub)
  implementation(libs.protobuf.kotlin)
  annotationProcessor(libs.spring.boot.configuration.processor)
}

stoveE2e.useDefaultStack()
stoveE2e.testLibrary("stove-spring")
stoveE2e.testLibrary("stove-wiremock")
stoveE2e.testLibrary("stove-grpc")
stoveE2e.testLibrary("stove-grpc-mock")
stoveE2e.testLibrary("ktor-client-websockets")
stoveE2e.testLibrary("ktor-client-okhttp")
stoveE2e.testLibrary("ktor-client-content-negotiation")
stoveE2e.testLibrary("ktor-serialization-jackson")
stoveE2e.testLibrary("testcontainers-kafka")

protobuf {
  protoc {
    artifact = libs.protoc.get().toString()
  }
  plugins {
    create("grpc") {
      artifact = libs.grpc.protoc.gen.java.get().toString()
    }
    create("grpckt") {
      artifact = "${libs.grpc.protoc.gen.kotlin.get()}:jdk8@jar"
    }
  }
  generateProtoTasks {
    all().forEach { task ->
      task.plugins {
        create("grpc")
        create("grpckt")
      }
      task.builtins {
        create("kotlin")
      }
    }
  }
}

val stoveServerUrl = providers.environmentVariable("STOVE_SERVER_URL").orElse("http://localhost:4040")

stoveTracing {
  serviceName.set("orders")
  testTaskNames.set(listOf("e2eTest"))
  otelAgentVersion.set(libs.versions.opentelemetry.instrumentation.get())
  profiling {
    enabled.set(true)
    serverUrl.set(stoveServerUrl)
    allocations.set(true)
  }
}

springBoot {
  mainClass.set("com.trendyol.stove.examples.kotlin.spring.ExampleStoveSpringBootAppKt")
}

stoveE2e.registerTest()
