import { randomUUID } from "node:crypto"
import type { IncomingMessage, ServerResponse } from "node:http"
import { context, propagation } from "@opentelemetry/api"
import type { Producer } from "kafkajs"
import type pg from "pg"
import type { Bridge } from "@stove-examples/stove-kafka"
import { send } from "@stove-examples/stove-kafka/kafkajs.mjs"
import { inSpan, readBody, writeJson } from "../http.js"

const TOPIC_CREATED = "product.created"

export function productHandlers(pool: pg.Pool, producer: Producer | null, bridge: Bridge) {
function productFromRow(row: { id: string; name: string; price: string | number }) {
  return { id: row.id, name: row.name, price: Number(row.price) }
}

async function createProduct(request: IncomingMessage, response: ServerResponse) {
  await inSpan(request, "POST /api/products", async () => {
    let body
    try {
      body = JSON.parse(await readBody(request))
    } catch {
      writeJson(response, 400, { error: "invalid request body" })
      return
    }
    if (!body || typeof body.name !== "string" || body.name.trim().length === 0 || typeof body.price !== "number") {
      writeJson(response, 400, { error: "invalid request body" })
      return
    }
    const product = { id: randomUUID(), name: body.name, price: body.price }
    await pool.query(
      "INSERT INTO products (id, name, price) VALUES ($1, $2, $3)",
      [product.id, product.name, product.price]
    )
    if (producer) {
      try {
        const headers: Record<string, string> = {}
        propagation.inject(context.active(), headers)
        if (request.headers.baggage && !headers.baggage) headers.baggage = Array.isArray(request.headers.baggage) ? request.headers.baggage[0] : request.headers.baggage
        if (request.headers.testcase) headers.testCase = Array.isArray(request.headers.testcase) ? request.headers.testcase[0] : request.headers.testcase
        await send(bridge, producer, TOPIC_CREATED, { key: product.id, value: JSON.stringify(product), headers })
      } catch (error) {
        console.warn("failed to publish product.created", error)
      }
    }
    writeJson(response, 201, product)
  })
}

async function getProduct(request: IncomingMessage, response: ServerResponse, id: string) {
  await inSpan(request, "GET /api/products/{id}", async () => {
    const result = await pool.query("SELECT id, name, price FROM products WHERE id = $1", [id])
    if (result.rowCount === 0) {
      writeJson(response, 404, { error: "not found" })
      return
    }
    writeJson(response, 200, productFromRow(result.rows[0]))
  })
}

async function listProducts(request: IncomingMessage, response: ServerResponse) {
  await inSpan(request, "GET /api/products", async () => {
    const result = await pool.query("SELECT id, name, price FROM products")
    writeJson(response, 200, result.rows.map(productFromRow))
  })
}

  return { createProduct, getProduct, listProducts }
}
