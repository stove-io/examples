package com.trendyol.stove.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.configureEach
import org.gradle.kotlin.dsl.withType

/**
 * Incubation of the Stove end-to-end test plugin.
 *
 * Projects apply `com.trendyol.stove.e2e`, configure [StoveE2eExtension], and register suites with
 * [StoveE2eExtension.registerTest]. A published plugin can keep this extension and resolve its
 * Stove version itself instead of reading the consumer catalog.
 */
class StoveE2ePlugin : Plugin<Project> {
  override fun apply(target: Project) {
    val extension = target.extensions.create("stoveE2e", StoveE2eExtension::class.java, target)
    extension.sourceSetName.convention("test-e2e")
    extension.kotlinSourceDirs.convention(emptyList())
    extension.resourceDirs.convention(emptyList())
    extension.jvmVersion.convention(21)

    target.pluginManager.apply("idea")
    target.tasks.withType<Test>().configureEach {
      maxParallelForks = 1
      useJUnitPlatform()
      jvmArgs("--add-opens", "java.base/java.util=ALL-UNNAMED")
      doFirst {
        val isolatedTemp = temporaryDir.resolve("tmp").apply { mkdirs() }
        systemProperty("java.io.tmpdir", isolatedTemp.absolutePath)
      }
    }
    target.afterEvaluate { extension.prepare() }
  }
}
