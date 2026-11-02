import http from "node:http"
import { startTelemetry } from "./telemetry.js"
import { inSpan, readBody, writeJson } from "./http.js"
import { productHandlers } from "./products/http.js"

import { context, propagation } from "@opentelemetry/api"
import { Kafka } from "kafkajs"
import pg from "pg"
import { bridgeFromEnv } from "@stove-examples/stove-kafka"
import { CheckoutError, CheckoutService } from "./orders/checkout.js"
import { streamOrders } from "./orders/stream.js"
import { startConfirmationWorker } from "./orders/confirmation-worker.js"
import { startOrderQueryServer, stopOrderQueryServer } from "./orders/query-grpc.js"
import { send } from "@stove-examples/stove-kafka/kafkajs.mjs"
import { consumeUpdates } from "./messaging/consumer.js"

const port = Number(process.env.APP_PORT || "8080")
const brokers = (process.env.KAFKA_BROKERS || "").split(",").map((broker) => broker.trim()).filter(Boolean)

const pool = new pg.Pool({
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || "5432"),
  database: process.env.DB_NAME || "stove",
  user: process.env.DB_USER || "stove",
  password: process.env.DB_PASS || "stove"
})

const bridge = bridgeFromEnv()
const telemetry = startTelemetry()
const kafka = brokers.length === 0 ? null : new Kafka({ clientId: "ecomm-node", brokers })
const producer = kafka ? kafka.producer() : null
const consumer = kafka ? kafka.consumer({ groupId: "ecomm-node" }) : null
const publish = async (topic: string, key: string, event: object, correlation: Record<string, string>) => {
  if (!producer) return
  const headers: Record<string, string> = { ...correlation }
  propagation.inject(context.active(), headers)
  await send(bridge, producer, topic, { key, value: JSON.stringify(event), headers })
}
if (producer) await producer.connect()
const confirmations = await startConfirmationWorker(pool, publish)
const checkout = new CheckoutService(pool, process.env.INVENTORY_URL || "http://localhost:8081", process.env.PAYMENT_URL || "http://localhost:8082", process.env.FRAUD_GRPC_TARGET || "localhost:9090", publish, (order, correlation) => confirmations.schedule({ orderId: order.orderId, userId: order.userId, productId: order.productId, amount: order.amount, correlation }))

const orderQueryServer = await startOrderQueryServer(pool, Number(process.env.ORDER_GRPC_PORT || "9091"))

const { createProduct, getProduct, listProducts } = productHandlers(pool, producer, bridge)

const server = http.createServer(async (request, response) => {
  try {
    const url = new URL(request.url || "/", "http://localhost")
    if (request.method === "GET" && url.pathname === "/health") {
      writeJson(response, 200, { status: "UP" })
      return
    }
    if (request.method === "POST" && url.pathname === "/api/products") {
      await createProduct(request, response)
      return
    }
    if (request.method === "POST" && url.pathname === "/api/orders") {
      await inSpan(request, "POST /api/orders", async () => {
        let body
        try { body = JSON.parse(await readBody(request)) } catch { writeJson(response, 400, { error: "invalid request body" }); return }
        if (!body || typeof body.userId !== "string" || typeof body.productId !== "string" || typeof body.amount !== "number") {
          writeJson(response, 400, { error: "invalid request body" }); return
        }
        const correlation: Record<string, string> = {}
        if (request.headers.testcase) correlation.testCase = Array.isArray(request.headers.testcase) ? request.headers.testcase[0] : request.headers.testcase
        if (request.headers.baggage) correlation.baggage = Array.isArray(request.headers.baggage) ? request.headers.baggage[0] : request.headers.baggage
        try { writeJson(response, 201, await checkout.create(body, correlation)) }
        catch (error) {
          if (error instanceof CheckoutError) writeJson(response, error.status, { message: error.message, errorCode: error.errorCode })
          else throw error
        }
      })
      return
    }
    if (request.method === "GET" && url.pathname === "/api/orders/stream") {
      await streamOrders(pool, url.searchParams.get("userId") || "", response)
      return
    }
    if (request.method === "GET" && url.pathname === "/api/products") {
      await listProducts(request, response)
      return
    }
    const orderMatch = url.pathname.match(/^\/api\/orders\/([^/]+)$/)
    if (request.method === "GET" && orderMatch) {
      const order = await checkout.find(decodeURIComponent(orderMatch[1]))
      if (order) writeJson(response, 200, order); else writeJson(response, 404, { error: "not found" })
      return
    }
    const match = url.pathname.match(/^\/api\/products\/([^/]+)$/)
    if (request.method === "GET" && match) {
      await getProduct(request, response, decodeURIComponent(match[1]))
      return
    }
    writeJson(response, 404, { error: "not found" })
  } catch (error) {
    console.error(error)
    if (!response.headersSent) writeJson(response, 500, { error: "internal error" })
  }
})

async function shutdown() {
  server.close()
  await stopOrderQueryServer(orderQueryServer)
  await confirmations.close()
  await consumer?.disconnect().catch(() => {})
  await producer?.disconnect().catch(() => {})
  bridge.close()
  checkout.close()
  await pool.end().catch(() => {})
  await telemetry?.shutdown().catch(() => {})
  process.exit(0)
}

process.on("SIGTERM", shutdown)
process.on("SIGINT", shutdown)

consumeUpdates(kafka, consumer, producer, pool, bridge).catch((error) => console.error("consumer stopped", error))
server.listen(port, "0.0.0.0", () => {
  console.log(`ecomm-node listening on ${port}`)
})
