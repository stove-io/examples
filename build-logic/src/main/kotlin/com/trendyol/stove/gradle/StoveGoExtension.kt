package com.trendyol.stove.gradle

import org.gradle.api.Project
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFile
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.register
import java.security.MessageDigest
import javax.inject.Inject

class GoProcessBinary(
  val task: TaskProvider<Exec>,
  val file: Provider<RegularFile>,
)

class GoContainerImage(
  val build: TaskProvider<Exec>,
  val remove: TaskProvider<Exec>,
  val reference: String,
)

/** Builds one Go module for a process suite or a container suite. */
abstract class StoveGoExtension @Inject constructor(
  private val project: Project,
) {
  abstract val module: DirectoryProperty
  abstract val executable: Property<String>
  abstract val dockerExecutable: Property<String>
  abstract val coverageEnabled: Property<Boolean>

  fun processBinary(name: String = "app"): GoProcessBinary {
    val binary = project.layout.buildDirectory.file(name)
    val moduleDir = moduleDir()
    val go = executable.get()
    val cover = coverageEnabled.get()
    val task = project.tasks.register<Exec>("buildGoApp") {
      description = "Compiles the Go application."
      group = "build"
      val output = binary.get().asFile
      workingDir = moduleDir
      val args = mutableListOf(go, "build")
      if (cover) args.add("-cover")
      args.addAll(listOf("-o", output.absolutePath, "."))
      commandLine(args)
      inputs.files(project.fileTree(moduleDir) { include("*.go", "go.mod", "go.sum") })
      inputs.property("goExecutable", go)
      inputs.property("coverage", cover)
      outputs.file(binary)
      doFirst { output.parentFile.mkdirs() }
    }
    return GoProcessBinary(task, binary)
  }

  fun containerImage(name: String): GoContainerImage {
    val checkoutId = MessageDigest.getInstance("SHA-256")
      .digest(project.rootDir.absolutePath.toByteArray())
      .take(8)
      .joinToString("") { "%02x".format(it) }
    val reference = "$name:$checkoutId"
    val moduleDir = moduleDir()
    val docker = dockerExecutable.get()
    val cover = coverageEnabled.get()
    val build = project.tasks.register<Exec>("buildGoImage") {
      description = "Builds the Go application image."
      group = "build"
      val buildFlags = if (cover) "-cover" else ""
      commandLine(
        docker,
        "build",
        "--file",
        moduleDir.resolve("Dockerfile").absolutePath,
        "--tag",
        reference,
        "--build-arg",
        "GO_BUILD_FLAGS=$buildFlags",
        moduleDir.absolutePath,
      )
      inputs.file(moduleDir.resolve("Dockerfile"))
      inputs.files(project.fileTree(moduleDir) { include("*.go", "go.mod", "go.sum") })
      inputs.property("coverageEnabled", cover)
      outputs.upToDateWhen { false }
    }
    val remove = project.tasks.register<Exec>("removeGoImage") {
      description = "Removes the local Go application image."
      group = "build"
      commandLine(docker, "image", "rm", reference)
      isIgnoreExitValue = true
    }
    return GoContainerImage(build, remove, reference)
  }

  fun applyCoverage(test: Test) {
    if (!coverageEnabled.get()) return
    test.systemProperty("go.cover.dir", coverageDir().absolutePath)
    test.outputs.cacheIf { false }
  }

  fun coverageReports(tests: Iterable<TaskProvider<out org.gradle.api.Task>>, description: String) {
    if (!coverageEnabled.get()) return
    val moduleDir = moduleDir()
    val coverDir = coverageDir()
    val coverOut = coverDir.resolve("coverage.out")
    val coverHtml = coverDir.resolve("coverage.html")
    val go = executable.get()
    val report = project.tasks.register<Exec>("goCoverageReport") {
      this.description = "Converts Go coverage data to standard format."
      group = "verification"
      workingDir = moduleDir
      mustRunAfter(tests)
      commandLine(go, "tool", "covdata", "textfmt", "-i=${coverDir.absolutePath}", "-o=${coverOut.absolutePath}")
    }
    val summary = project.tasks.register<Exec>("goCoverageSummary") {
      this.description = "Prints Go coverage summary."
      group = "verification"
      workingDir = moduleDir
      dependsOn(report)
      commandLine(go, "tool", "cover", "-func=${coverOut.absolutePath}")
    }
    val html = project.tasks.register<Exec>("goCoverageHtml") {
      this.description = "Generates HTML coverage report."
      group = "verification"
      workingDir = moduleDir
      dependsOn(report)
      commandLine(go, "tool", "cover", "-html=${coverOut.absolutePath}", "-o=${coverHtml.absolutePath}")
      doLast { project.logger.lifecycle("Go coverage HTML: ${coverHtml.absolutePath}") }
    }
    project.tasks.register("e2eTestWithCoverage") {
      this.description = description
      group = "verification"
      dependsOn(tests)
      finalizedBy(summary, html)
    }
  }

  private fun moduleDir() = module.asFile.get()

  private fun coverageDir() = project.layout.buildDirectory.dir("go-coverage").get().asFile
}
