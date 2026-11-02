import test from "node:test"
import assert from "node:assert/strict"
import grpc from "@grpc/grpc-js"
import protoLoader from "@grpc/proto-loader"
import { bridgeFromEnv } from "@stove-examples/stove-kafka"
import { send, received, commit } from "@stove-examples/stove-kafka/kafkajs.mjs"

const service = grpc.loadPackageDefinition(protoLoader.loadSync(new URL("../../stove-kafka/messages.proto", import.meta.url).pathname, { longs: String, defaults: true })).com.trendyol.stove.kafka.StoveKafkaObserverService

test("bridge reports actual Kafka outcomes with exact offsets and headers", async () => {
  const records = []
  const server = new grpc.Server()
  const handle = kind => (call, done) => { records.push([kind, call.request]); done(null, { status: 200 }) }
  server.addService(service.service, { onPublishedMessage: handle("published"), onConsumedMessage: handle("consumed"), onCommittedMessage: handle("committed"), onAcknowledgedMessage: handle("acknowledged") })
  const port = await new Promise((resolve, reject) => server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, p) => err ? reject(err) : resolve(p)))
  const bridge = bridgeFromEnv({ STOVE_KAFKA_BRIDGE_PORT: String(port), STOVE_KAFKA_BRIDGE_HOST: "127.0.0.1" })
  try {
    const msg = { key: "id", value: Buffer.from([0, 255]), headers: { testCase: "case" } }
    await send(bridge, { send: async () => [{ partition: 2, baseOffset: "9007199254740993" }] }, "products", msg)
    await received(bridge, "products", 2, { ...msg, offset: "9007199254740993" })
    await commit(bridge, { commitOffsets: async offsets => assert.equal(offsets[0].offset, "9007199254740994") }, "products", 2, "9007199254740993")
    assert.deepEqual(records.map(r => r[0]), ["published", "acknowledged", "consumed", "committed"])
    assert.deepEqual(records[0][1].message, msg.value)
    assert.equal(records[2][1].headers.testCase, "case")
    assert.equal(records[2][1].offset, "9007199254740993")
    assert.equal(records[3][1].offset, "9007199254740994")
    await received(bridge, "products", 2, { key: null, value: null, headers: { nullHeader: null }, offset: "1" })
    assert.deepEqual(records.at(-1)[1].message, Buffer.alloc(0))
    assert.equal(records.at(-1)[1].key, "")
    assert.equal(records.at(-1)[1].headers.nullHeader, "")
    await assert.rejects(send(bridge, { send: async () => { throw Error("send failed") } }, "products", msg))
    assert.equal(records.at(-1)[0], "acknowledged")
    const count = records.length
    await assert.rejects(commit(bridge, { commitOffsets: async () => { throw Error("commit failed") } }, "products", 2, "1"))
    assert.equal(records.length, count)
  } finally {
    bridge.close()
    await new Promise(resolve => server.tryShutdown(resolve))
  }
})

test("observer calls time out and the bridge closes", async () => {
  const server = new grpc.Server()
  const hang = () => {}
  server.addService(service.service, { onPublishedMessage: hang, onConsumedMessage: hang, onCommittedMessage: hang, onAcknowledgedMessage: hang })
  const port = await new Promise((resolve, reject) => server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (error, p) => error ? reject(error) : resolve(p)))
  const bridge = bridgeFromEnv({ STOVE_KAFKA_BRIDGE_PORT: String(port), STOVE_KAFKA_BRIDGE_HOST: "127.0.0.1" })
  try {
    const start = Date.now()
    const result = await send(bridge, { send: async () => [] }, "test", { value: "body" })
    assert.deepEqual(result, [])
    assert.ok(Date.now() - start >= 900 && Date.now() - start < 3000)
  } finally {
    bridge.close()
    server.forceShutdown()
  }
})

test("disabled and unavailable observers do not affect application operations", async () => {
  const disabled = bridgeFromEnv({})
  await disabled.published({ topic: "test" })
  disabled.close()
  const unavailable = bridgeFromEnv({ STOVE_KAFKA_BRIDGE_PORT: "1" })
  const result = await send(unavailable, { send: async () => [] }, "test", { value: "body" })
  assert.deepEqual(result, [])
  unavailable.close()
})
