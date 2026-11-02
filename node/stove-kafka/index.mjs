import { randomUUID } from "node:crypto"
import { fileURLToPath } from "node:url"
import grpc from "@grpc/grpc-js"
import protoLoader from "@grpc/proto-loader"

const protoPath = fileURLToPath(new URL("./messages.proto", import.meta.url))

// The observer is optional: production applications need no running Stove process.
export function bridgeFromEnv(env = process.env) {
  if (!env.STOVE_KAFKA_BRIDGE_PORT) return new KafkaBridge(null)
  const definition = protoLoader.loadSync(protoPath, { longs: String, defaults: true })
  const service = grpc.loadPackageDefinition(definition).com.trendyol.stove.kafka.StoveKafkaObserverService
  const client = new service(`${env.STOVE_KAFKA_BRIDGE_HOST || "localhost"}:${env.STOVE_KAFKA_BRIDGE_PORT}`, grpc.credentials.createInsecure())
  return new KafkaBridge(client)
}

export class KafkaBridge {
  constructor(client) { this.client = client }
  async report(method, message) {
    if (!this.client) return
    try {
      await new Promise((resolve, reject) => this.client[method]({ id: randomUUID(), ...message }, { deadline: Date.now() + 1000 }, (error, reply) => error ? reject(error) : resolve(reply)))
    } catch (error) {
      console.warn(`Stove Kafka bridge ${method}: ${error.message}`)
    }
  }
  published(message) { return this.report("onPublishedMessage", message) }
  consumed(message) { return this.report("onConsumedMessage", message) }
  committed(message) { return this.report("onCommittedMessage", message) }
  acknowledged(message) { return this.report("onAcknowledgedMessage", message) }
  close() { this.client?.close() }
}

export function textHeaders(headers = {}) {
  return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, (Array.isArray(value) ? value.at(-1) : value)?.toString("utf8") ?? ""]))
}
