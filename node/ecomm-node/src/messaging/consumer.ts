import { context, propagation, SpanKind, trace } from "@opentelemetry/api"
import type { Kafka, Consumer, Producer } from "kafkajs"
import type pg from "pg"
import type { Bridge } from "@stove-examples/stove-kafka"
import { send, received, commit } from "@stove-examples/stove-kafka/kafkajs.mjs"
import { updateStatistics } from "../orders/statistics.js"

const TOPIC_CREATED = "product.created"
const TOPIC_UPDATE = "product.update"
const TOPIC_UPDATED = "product.updated"

export async function consumeUpdates(kafka: Kafka | null, consumer: Consumer | null, producer: Producer | null, pool: pg.Pool, bridge: Bridge) {
  if (!consumer || !kafka) return
  const admin = kafka.admin()
  try {
    await admin.connect()
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic: TOPIC_CREATED, numPartitions: 1, replicationFactor: 1 },
        { topic: TOPIC_UPDATE, numPartitions: 1, replicationFactor: 1 },
        { topic: "showcase.orders.created", numPartitions: 1, replicationFactor: 1 }
      ]
    })
    await consumer.connect()
    await consumer.subscribe({ topics: [TOPIC_UPDATE, "showcase.orders.created"], fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        await received(bridge, topic, partition, message)
        const incoming = Object.fromEntries(Object.entries(message.headers ?? {}).map(([key, value]) => [key, (Array.isArray(value) ? value.at(-1) : value)?.toString() ?? ""]))
        const parent = propagation.extract(context.active(), incoming)
        const span = trace.getTracer("ecomm-node").startSpan(`process ${topic}`, { kind: SpanKind.CONSUMER }, parent)
        try {
          await context.with(trace.setSpan(parent, span), async () => {
            try {
              const event = JSON.parse(message.value?.toString("utf8") || "")
              if (topic === "showcase.orders.created") {
                if (typeof event.orderId !== "string" || typeof event.userId !== "string" || typeof event.amount !== "number") throw new SyntaxError("invalid order event")
                await updateStatistics(pool, event)
                if (producer) {
                  const headers = { ...message.headers }
                  propagation.inject(context.active(), headers)
                  await send(bridge, producer, "customer.statistics.updated", { key: event.userId, value: JSON.stringify({ orderId: event.orderId, userId: event.userId }), headers })
                }
              } else {
              const result = await pool.query("UPDATE products SET name = $1, price = $2 WHERE id = $3", [event.name, event.price, event.id])
              if (result.rowCount && producer) {
                const headers = { ...message.headers }
                propagation.inject(context.active(), headers)
                await send(bridge, producer, TOPIC_UPDATED, { key: event.id, value: JSON.stringify(event), headers })
              }
              }
            } catch (error) {
              if (!(error instanceof SyntaxError)) { console.error("product.update failed", error); throw error }
              console.warn("ignored product.update payload", error)
            }
            await commit(bridge, consumer, topic, partition, message.offset)
          })
        } finally {
          span.end()
        }
      }
    })
  } catch (error) {
    console.error("kafka consumer failed", error)
  } finally {
    await admin.disconnect().catch(() => {})
  }
}
