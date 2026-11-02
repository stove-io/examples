plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.stove.tracing)
  id("com.trendyol.stove.e2e")
}

stoveE2e {
  sourceSetName.set("stovetests")
  kotlinSourceDirs.set(listOf("stovetests/kotlin", "../../usecase/e2e/kotlin", "../ecomm-jobs/stovetests/kotlin"))
  resourceDirs.set(listOf("stovetests/resources"))
}

dependencies {
  implementation(libs.ktor.server.core)
  implementation(libs.ktor.server.netty)
  implementation(libs.ktor.server.content.negotiation)
  implementation(libs.ktor.serialization.jackson)
  implementation(libs.postgresql)
  implementation(libs.kafka.clients)
  implementation(libs.opentelemetry.api)
  implementation(libs.logback.classic)
  implementation(libs.jackson.module.kotlin)
  implementation(project(":protocol:commerce-jvm"))
  implementation(project(":jvm:ecomm-jobs"))
  implementation(libs.grpc.protobuf)
  implementation(libs.grpc.stub)
  implementation(libs.grpc.netty)
  compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

stoveE2e.useDefaultStack()
dependencies { testImplementation(project(":protocol:commerce-jvm")) }
stoveE2e.testLibrary("stove-ktor")

stoveTracing {
  serviceName.set("ecomm-ktor")
  testTaskNames.set(listOf("e2eTest"))
  otelAgentVersion.set(libs.versions.opentelemetry.instrumentation.get())
  profiling {
    enabled.set(true)
    serverUrl.set(providers.environmentVariable("STOVE_SERVER_URL").orElse("http://localhost:4040"))
    allocations.set(true)
  }
}

stoveE2e.registerTest()
