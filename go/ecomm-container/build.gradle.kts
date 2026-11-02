plugins {
  alias(libs.plugins.kotlin.jvm)
  id("com.trendyol.stove.e2e")
  id("com.trendyol.stove.go")
}

stoveE2e {
  sourceSetName.set("stovetests")
  kotlinSourceDirs.set(listOf("stovetests/kotlin", "../../usecase/e2e/kotlin"))
  resourceDirs.set(listOf("stovetests/resources"))
}

stoveGo {
  module.set(layout.projectDirectory.dir("../ecomm"))
}

stoveE2e.useDefaultStack()
dependencies { testImplementation(project(":protocol:commerce-jvm")) }
stoveE2e.testLibrary("stove-container")

val image = stoveGo.containerImage("ecomm")

val e2eTest = stoveE2e.registerTest("e2eTest") {
  description = "Runs the e-commerce container suite."
  dependsOn(image.build)
  finalizedBy(image.remove)
  systemProperty("go.app.container.image", image.reference)
  stoveGo.applyCoverage(this)
}

stoveGo.coverageReports(
  listOf(e2eTest),
  "Runs the e-commerce container suite and generates a Go coverage report.",
)
