package com.trendyol.stove.examples.ecomm.ktor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.trendyol.stove.examples.ecomm.ktor.checkout.Checkout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

private const val TOPIC_CREATED = "product.created"
private const val TOPIC_UPDATE = "product.update"
private val json = jacksonObjectMapper()

internal class Messaging(config: Map<String, String>, private val database: Database, private val checkout: Checkout) {
  private val producer: KafkaProducer<String, String>?
  private val consumer: KafkaConsumer<String, String>?
  private val consumerThread: Thread?
  private val running = AtomicBoolean(true)

  init {
    val brokers = config["kafka.bootstrapServers"].orEmpty()
    if (brokers.isBlank()) {
      producer = null
      consumer = null
      consumerThread = null
    } else {
      val interceptor = config["kafka.interceptorClasses"].orEmpty()
      producer = KafkaProducer(producerProperties(brokers, interceptor))
      consumer = KafkaConsumer(consumerProperties(brokers, interceptor))
      consumer.subscribe(listOf(TOPIC_UPDATE, "showcase.orders.created"))
      consumerThread = thread(name = "ecomm-ktor-updates", isDaemon = true) {
        while (running.get()) {
          val records = try {
            consumer.poll(Duration.ofMillis(200))
          } catch (_: Exception) {
            if (!running.get()) break else continue
          }
          for (record in records) {
            val partition = TopicPartition(record.topic(), record.partition())
            try {
              if (record.topic() == "showcase.orders.created") {
                val completion = checkout.updateStatistics(record.value())
                if (completion != null) producer.send(ProducerRecord("customer.statistics.updated", null, record.key(), completion, record.headers())).get()
              } else {
                val event = try { json.readValue<ProductEvent>(record.value()) } catch (_: Exception) { null }
                if (event != null && database.update(event.id, event.name, event.price)) {
                  producer.send(ProducerRecord("product.updated", null, event.id, record.value(), record.headers())).get()
                }
              }
              consumer.commitSync(mapOf(partition to OffsetAndMetadata(record.offset() + 1)))
            } catch (error: Exception) {
              if (!running.get()) break
              System.err.println("product update failed: $error")
              consumer.seek(partition, record.offset())
              break
            }
          }
        }
        consumer.close()
      }
    }
  }

  fun publish(topic: String, key: String, payload: String, headers: Map<String, String> = emptyMap()) {
    val record = ProducerRecord(topic, key, payload)
    headers.forEach { (name, value) -> record.headers().add(name, value.toByteArray(Charsets.UTF_8)) }
    producer?.send(record)?.get() ?: error("Kafka producer is unavailable")
  }

  fun publishCreated(product: Product) {
    val current = producer ?: return
    val payload = json.writeValueAsString(ProductEvent(product.id, product.name, product.price))
    try {
      current.send(ProducerRecord(TOPIC_CREATED, product.id, payload)).get()
    } catch (_: Exception) {
      // The create response stays 201 when the broker write fails.
    }
  }

  fun close() {
    running.set(false)
    consumer?.wakeup()
    consumerThread?.join(2_000)
    producer?.close()
  }

  private fun producerProperties(brokers: String, interceptor: String) = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    put(ProducerConfig.ACKS_CONFIG, "1")
    if (interceptor.isNotBlank()) put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor)
  }

  private fun consumerProperties(brokers: String, interceptor: String) = Properties().apply {
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    put(ConsumerConfig.GROUP_ID_CONFIG, "ecomm-ktor")
    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    if (interceptor.isNotBlank()) put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor)
  }
}
