plugins {
  alias(libs.plugins.kotlin.jvm)
  id("com.trendyol.stove.e2e")
  id("com.trendyol.stove.go")
}

stoveE2e {
  sourceSetName.set("stovetests")
  kotlinSourceDirs.set(listOf("stovetests/kotlin", "../catalog/e2e/kotlin"))
  resourceDirs.set(listOf("stovetests/resources"))
}

stoveGo {
  module.set(layout.projectDirectory.dir("../catalog"))
}

stoveE2e.useDefaultStack()
stoveE2e.testLibrary("stove-process")

val binary = stoveGo.processBinary("catalog")
val kafkaLibraries = listOf("sarama", "franz", "segmentio")

val kafkaE2eTasks = kafkaLibraries.mapIndexed { index, lib ->
  stoveE2e.registerTest("e2eTest_$lib") {
    description = "Runs the catalog process suite with the $lib Kafka library."
    dependsOn(binary.task)
    inputs.file(binary.file)
    systemProperty("go.app.binary", binary.file.get().asFile.absolutePath)
    systemProperty("kafka.library", lib)
    stoveGo.applyCoverage(this)
    if (index > 0) mustRunAfter("e2eTest_${kafkaLibraries[index - 1]}")
  }
}

stoveE2e.registerTest("e2eTest") {
  description = "Runs the catalog process suite for every Kafka library."
  dependsOn(kafkaE2eTasks)
  enabled = false
}

stoveGo.coverageReports(
  kafkaE2eTasks,
  "Runs the catalog process suite and generates a Go coverage report.",
)
