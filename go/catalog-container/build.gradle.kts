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
stoveE2e.testLibrary("stove-container")

val image = stoveGo.containerImage("catalog")

val e2eTest = stoveE2e.registerTest("e2eTest") {
  description = "Runs the catalog container suite."
  dependsOn(image.build)
  finalizedBy(image.remove)
  systemProperty("go.app.container.image", image.reference)
  systemProperty("kafka.library", "sarama")
  stoveGo.applyCoverage(this)
}

stoveGo.coverageReports(
  listOf(e2eTest),
  "Runs the catalog container suite and generates a Go coverage report.",
)
