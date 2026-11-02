package com.trendyol.stove.examples.ecomm.e2e.tests

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.trendyol.stove.serialization.StoveSerde

/** JVM Kafka interceptors observe String values as JSON strings; unwrap them for assertions. */
class EcommKafkaSerde : StoveSerde<Any, ByteArray> {
  private val delegate = StoveSerde.jackson.anyByteArraySerde()
  private val mapper = jacksonObjectMapper()

  override fun serialize(value: Any): ByteArray = delegate.serialize(value)

  override fun <T : Any> deserialize(value: ByteArray, clazz: Class<T>): T {
    val bytes = if (value.firstOrNull() == '"'.code.toByte()) {
      mapper.readValue(value, String::class.java).toByteArray(Charsets.UTF_8)
    } else value
    if (clazz == ByteArray::class.java) return clazz.cast(bytes)
    return delegate.deserialize(bytes, clazz)
  }
}
