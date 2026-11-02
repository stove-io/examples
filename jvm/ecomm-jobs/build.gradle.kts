plugins { `java-library` }

java { toolchain.languageVersion.set(JavaLanguageVersion.of(21)) }

dependencies {
  api("com.github.kagkarlsson:db-scheduler:${libs.versions.db.scheduler.get()}")
}
