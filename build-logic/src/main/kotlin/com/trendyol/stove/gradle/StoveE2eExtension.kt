package com.trendyol.stove.gradle

import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import javax.inject.Inject

/** Source set, toolchain, and catalog-backed test dependencies shared by every Stove suite. */
abstract class StoveE2eExtension @Inject constructor(
  private val project: Project,
) {
  abstract val sourceSetName: Property<String>
  abstract val kotlinSourceDirs: ListProperty<String>
  abstract val resourceDirs: ListProperty<String>
  abstract val jvmVersion: Property<Int>

  private var prepared = false

  /**
   * Registers a [Test] task on the e2e source set.
   * Call this after configuring [sourceSetName] and the source directories.
   */
  fun registerTest(name: String = "e2eTest", configure: Action<Test> = Action {}): org.gradle.api.tasks.TaskProvider<Test> {
    prepare()
    val extension = this
    return project.tasks.register<Test>(name) {
      description = "Runs e2e tests."
      group = "verification"
      val sourceSet = project.extensions.getByType<SourceSetContainer>().getByName(extension.sourceSetName.get())
      testClassesDirs = sourceSet.output.classesDirs
      classpath = sourceSet.runtimeClasspath
      useJUnitPlatform()
      reports {
        junitXml.required.set(true)
        html.required.set(true)
      }
      configure.execute(this)
    }
  }

  /** Stove BOM, Kotest, and the systems every example suite uses. */
  fun useDefaultStack() {
    prepare()
    testPlatform("stove-bom")
    listOf(
      "stove",
      "stove-postgres",
      "stove-http",
      "stove-tracing",
      "stove-dashboard",
      "stove-kafka",
      "stove-extensions-kotest",
      "arrow-core",
      "kotest-runner-junit6",
      "kotest-framework-engine",
      "kotest-assertions-core",
    ).forEach(::testLibrary)
  }

  /** Adds a `libs` version-catalog alias to `testImplementation`. */
  fun testLibrary(alias: String) {
    prepare()
    project.dependencies.add("testImplementation", catalogLibrary(alias))
  }

  fun testPlatform(alias: String) {
    prepare()
    project.dependencies.add("testImplementation", project.dependencies.platform(catalogLibrary(alias)))
  }

  internal fun prepare() {
    if (prepared) return
    prepared = true

    val name = sourceSetName.get()
    val kotlinDirs = kotlinSourceDirs.get().ifEmpty { listOf("src/$name/kotlin") }
    val resources = resourceDirs.get().ifEmpty { listOf("src/$name/resources") }
    val sourceSets = project.extensions.getByType<SourceSetContainer>()
    val sourceSet = sourceSets.findByName(name) ?: sourceSets.create(name)
    val kotlin = sourceSet.extensions.findByName("kotlin") as? SourceDirectorySet
      ?: error("Apply org.jetbrains.kotlin.jvm before com.trendyol.stove.e2e.")
    kotlin.setSrcDirs(kotlinDirs)
    sourceSet.resources.setSrcDirs(resources)
    sourceSet.compileClasspath += sourceSets.getByName("main").output
    sourceSet.runtimeClasspath += sourceSets.getByName("main").output

    project.configurations.named(sourceSet.implementationConfigurationName).configure {
      extendsFrom(project.configurations.getByName("testImplementation"))
    }
    project.configurations.named(sourceSet.runtimeOnlyConfigurationName).configure {
      extendsFrom(project.configurations.getByName("runtimeOnly"))
    }

    project.extensions.configure(JavaPluginExtension::class.java) {
      toolchain.languageVersion.set(org.gradle.jvm.toolchain.JavaLanguageVersion.of(jvmVersion.get()))
    }
    project.extensions.configure(KotlinJvmProjectExtension::class.java) {
      jvmToolchain(jvmVersion.get())
    }
    project.extensions.configure(IdeaModel::class.java) {
      module.testSources.from(sourceSet.allSource.sourceDirectories)
      module.testResources.from(sourceSet.resources.sourceDirectories)
    }
  }

  private fun catalogLibrary(alias: String): Any {
    val catalog = project.extensions.getByType<VersionCatalogsExtension>().named("libs")
    return catalog.findLibrary(alias).orElseThrow {
      IllegalArgumentException("Version catalog 'libs' has no library '$alias'.")
    }.get()
  }
}
