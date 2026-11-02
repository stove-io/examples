plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spring.boot)
  alias(libs.plugins.stove.tracing)
  id("com.trendyol.stove.e2e")
}

stoveE2e {
  sourceSetName.set("stovetests")
  kotlinSourceDirs.set(listOf("stovetests/kotlin", "../../usecase/e2e/kotlin", "../ecomm-jobs/stovetests/kotlin"))
  resourceDirs.set(listOf("stovetests/resources"))
}

dependencies {
  implementation(libs.spring.boot.web)
  implementation(libs.spring.boot.jdbc)
  implementation(libs.spring.kafka)
  implementation(libs.postgresql)
  implementation(libs.opentelemetry.api)
  implementation(project(":protocol:commerce-jvm"))
  implementation(project(":jvm:ecomm-jobs"))
  implementation(libs.grpc.protobuf)
  implementation(libs.grpc.stub)
  implementation(libs.grpc.netty)
  compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

stoveE2e.useDefaultStack()
dependencies { testImplementation(project(":protocol:commerce-jvm")) }
stoveE2e.testLibrary("stove-spring")

stoveTracing {
  serviceName.set("ecomm-java")
  testTaskNames.set(listOf("e2eTest"))
  otelAgentVersion.set(libs.versions.opentelemetry.instrumentation.get())
  profiling {
    enabled.set(true)
    serverUrl.set(providers.environmentVariable("STOVE_SERVER_URL").orElse("http://localhost:4040"))
    allocations.set(true)
  }
}

springBoot {
  mainClass.set("com.trendyol.stove.examples.ecomm.java.EcommApplication")
}

stoveE2e.registerTest()
