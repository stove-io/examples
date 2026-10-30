package com.trendyol.stove.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * Incubation of the Go application tasks a Stove plugin can later publish:
 * process binary, container image, and coverage reports for a shared module.
 */
class StoveGoPlugin : Plugin<Project> {
  override fun apply(target: Project) {
    val extension = target.extensions.create("stoveGo", StoveGoExtension::class.java, target)
    extension.executable.convention(target.providers.environmentVariable("GO_EXECUTABLE").orElse("go"))
    extension.dockerExecutable.convention(target.providers.environmentVariable("DOCKER_EXECUTABLE").orElse("docker"))
    extension.coverageEnabled.convention(
      target.providers.gradleProperty("go.coverage").map(String::toBoolean).orElse(false),
    )
  }
}
