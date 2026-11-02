declare module "@stove-examples/stove-kafka" {
  export interface Bridge {
    close(): void
  }
  export function bridgeFromEnv(): Bridge
}

declare module "@stove-examples/stove-kafka/kafkajs.mjs" {
  import type { Consumer, KafkaMessage, Message, Producer, RecordMetadata } from "kafkajs"
  import type { Bridge } from "@stove-examples/stove-kafka"
  export function send(bridge: Bridge, producer: Producer, topic: string, message: Message): Promise<RecordMetadata[]>
  export function received(bridge: Bridge, topic: string, partition: number, message: KafkaMessage): Promise<void>
  export function commit(bridge: Bridge, consumer: Consumer, topic: string, partition: number, offset: string): Promise<void>
}
