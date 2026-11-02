plugins {
  alias(libs.plugins.kotlin.jvm)
  id("com.trendyol.stove.e2e")
  id("com.trendyol.stove.go")
}

stoveE2e {
  sourceSetName.set("stovetests")
  kotlinSourceDirs.set(listOf("stovetests/kotlin", "../../usecase/e2e/kotlin", "../../usecase/process/kotlin"))
  resourceDirs.set(listOf("stovetests/resources"))
}

stoveGo {
  module.set(layout.projectDirectory.dir("../ecomm"))
}

stoveE2e.useDefaultStack()
dependencies { testImplementation(project(":protocol:commerce-jvm")) }
stoveE2e.testLibrary("stove-process")

val binary = stoveGo.processBinary("ecomm")
val e2eTest = stoveE2e.registerTest("e2eTest") {
  description = "Runs the e-commerce process suite with franz-go."
  dependsOn(binary.task)
  inputs.file(binary.file)
  systemProperty("go.app.binary", binary.file.get().asFile.absolutePath)
  stoveGo.applyCoverage(this)
}

stoveGo.coverageReports(
  listOf(e2eTest),
  "Runs the e-commerce process suite and generates a Go coverage report.",
)
