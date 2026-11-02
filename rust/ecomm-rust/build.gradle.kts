plugins {
  alias(libs.plugins.kotlin.jvm)
  id("com.trendyol.stove.e2e")
}

stoveE2e {
  sourceSetName.set("stovetests")
  kotlinSourceDirs.set(listOf("stovetests/kotlin", "../../usecase/e2e/kotlin", "../../usecase/process/kotlin"))
  resourceDirs.set(listOf("stovetests/resources"))
}

stoveE2e.useDefaultStack()
dependencies { testImplementation(project(":protocol:commerce-jvm")) }
stoveE2e.testLibrary("stove-process")

val binaryName = if (System.getProperty("os.name").lowercase().contains("win")) "ecomm-rust.exe" else "ecomm-rust"
val binary = layout.projectDirectory.file("target/debug/$binaryName")

val cargoBuild = tasks.register<Exec>("cargoBuild") {
  group = "build"
  description = "Builds the e-commerce Rust binary."
  workingDir = layout.projectDirectory.asFile
  commandLine("cargo", "build", "--locked")
  inputs.files("Cargo.toml", "Cargo.lock", "build.rs", fileTree("../../protocol/commerce"), fileTree("src"), "../stove-kafka/Cargo.toml", "../stove-kafka/build.rs", fileTree("../stove-kafka/src"), "../../protocol/messages.proto")
  outputs.file(binary)
}

stoveE2e.registerTest("e2eTest") {
  description = "Runs the e-commerce Rust process suite."
  dependsOn(cargoBuild)
  systemProperty("rust.app.binary", binary.asFile.absolutePath)
}
