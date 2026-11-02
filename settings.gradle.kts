pluginManagement {
  includeBuild("build-logic")
  repositories {
    gradlePluginPortal()
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots")
  }
}

plugins {
  id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "stove-examples"

dependencyResolutionManagement {
  repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
  repositories {
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots") {
      content {
        includeGroup("com.trendyol")
        includeGroupByRegex("com\\.trendyol\\..*")
      }
    }
  }
}

include(
  "protocol:commerce-jvm",
  "jvm:ecomm-jobs",
  "go:ecomm-process",
  "go:ecomm-container",
  "jvm:ecomm-ktor",
  "jvm:ecomm-java",
  "node:ecomm-node",
  "rust:ecomm-rust",
)
