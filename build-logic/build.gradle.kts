import org.gradle.api.artifacts.VersionCatalogsExtension

plugins {
  `kotlin-dsl`
}

repositories {
  mavenCentral()
  gradlePluginPortal()
}

val libsCatalog = extensions.getByType(VersionCatalogsExtension::class.java).named("libs")
val kotlinVersion = libsCatalog.findVersion("kotlin").get().requiredVersion

dependencies {
  implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:$kotlinVersion")
}

gradlePlugin {
  plugins {
    create("stoveE2e") {
      id = "com.trendyol.stove.e2e"
      implementationClass = "com.trendyol.stove.gradle.StoveE2ePlugin"
    }
    create("stoveGo") {
      id = "com.trendyol.stove.go"
      implementationClass = "com.trendyol.stove.gradle.StoveGoPlugin"
    }
  }
}
