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

val npmInstall = tasks.register<Exec>("npmInstall") {
  group = "build"
  description = "Installs the e-commerce Node.js dependencies."
  workingDir = layout.projectDirectory.asFile
  commandLine("npm", "ci", "--ignore-scripts", "--install-links")
  inputs.files("package.json", "package-lock.json", fileTree("../stove-kafka"))
  outputs.dir("node_modules")
}

val npmBuild = tasks.register<Exec>("npmBuild") {
  dependsOn(npmInstall)
  workingDir = layout.projectDirectory.asFile
  commandLine("npm", "run", "build")
  inputs.files(fileTree("src"), fileTree("../../protocol/commerce"), "tsconfig.json")
  outputs.dir("dist")
}

stoveE2e.registerTest("e2eTest") {
  description = "Runs the e-commerce Node.js process suite."
  dependsOn(npmBuild)
  systemProperty("node.app.script", layout.projectDirectory.file("dist/server.js").asFile.absolutePath)
}
