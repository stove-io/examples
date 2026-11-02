plugins {
  `java-library`
  alias(libs.plugins.protobuf)
}

java { toolchain.languageVersion.set(JavaLanguageVersion.of(21)) }

val protobufVersion = libs.versions.protobuf.asProvider().get()

dependencies {
  api("com.google.protobuf:protobuf-java") { version { strictly(protobufVersion) } }
  api(libs.grpc.protobuf)
  api(libs.grpc.stub)
  compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

sourceSets.main { proto { srcDir("../commerce") } }
protobuf {
  protoc { artifact = libs.protoc.get().toString() }
  plugins { create("grpc") { artifact = libs.grpc.protoc.gen.java.get().toString() } }
  generateProtoTasks { all().forEach { task -> task.plugins { create("grpc") } } }
}
