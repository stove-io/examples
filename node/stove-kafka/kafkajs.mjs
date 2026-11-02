import { textHeaders } from "./index.mjs"

// Wrappers report real Kafka outcomes and never turn observer errors into Kafka errors.
export async function send(bridge, producer, topic, message) {
  let metadata
  try {
    metadata = await producer.send({ topic, messages: [message] })
  } catch (error) {
    await bridge.acknowledged({ topic, partition: -1, offset: "-1", exception: String(error.message) })
    throw error
  }
  await bridge.published({ topic, key: message.key?.toString() ?? "", message: Buffer.from(message.value ?? ""), headers: textHeaders(message.headers) })
  for (const item of metadata) await bridge.acknowledged({ topic, partition: item.partition, offset: item.baseOffset ?? "-1", exception: "" })
  return metadata
}

export async function received(bridge, topic, partition, message) {
  await bridge.consumed({ topic, partition, offset: message.offset, key: message.key?.toString() ?? "", message: message.value ?? Buffer.alloc(0), headers: textHeaders(message.headers) })
}

export async function commit(bridge, consumer, topic, partition, offset) {
  const next = (BigInt(offset) + 1n).toString()
  await consumer.commitOffsets([{ topic, partition, offset: next }])
  await bridge.committed({ topic, partition, offset: next, metadata: "" })
}
