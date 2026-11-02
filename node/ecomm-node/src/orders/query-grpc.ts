import { fileURLToPath } from "node:url"
import * as grpc from "@grpc/grpc-js"
import * as protoLoader from "@grpc/proto-loader"
import type pg from "pg"

interface OrderRow {
  id: string
  user_id: string
  product_id: string
  amount: string
  status: string
  payment_transaction_id: string | null
  created_at: Date
}

function orderProto(row: OrderRow) {
  return {
    id: row.id,
    userId: row.user_id,
    productId: row.product_id,
    amount: Number(row.amount),
    status: row.status,
    paymentTransactionId: row.payment_transaction_id ?? "",
    createdAt: String(row.created_at.getTime())
  }
}

/** Exposes persisted orders through the same contract used by every commerce app. */
export async function startOrderQueryServer(pool: pg.Pool, port: number): Promise<grpc.Server> {
  const path = fileURLToPath(new URL("../proto/order_query.proto", import.meta.url))
  const definition = protoLoader.loadSync(path, { longs: String, defaults: true })
  const loaded = grpc.loadPackageDefinition(definition) as unknown as {
    orderquery: { OrderQueryService: { service: grpc.ServiceDefinition } }
  }
  const server = new grpc.Server()
  server.addService(loaded.orderquery.OrderQueryService.service, {
    getOrder: async (call: grpc.ServerUnaryCall<{ orderId: string }, object>, callback: grpc.sendUnaryData<object>) => {
      try {
        const result = await pool.query<OrderRow>("SELECT id, user_id, product_id, amount, status, payment_transaction_id, created_at FROM orders WHERE id = $1", [call.request.orderId])
        callback(null, result.rows[0] ? { found: true, order: orderProto(result.rows[0]) } : { found: false })
      } catch (error) {
        console.error("Order query failed", error)
        callback({ code: grpc.status.INTERNAL, message: "Order query failed" })
      }
    },
    getOrdersByUser: async (call: grpc.ServerUnaryCall<{ userId: string }, object>, callback: grpc.sendUnaryData<object>) => {
      try {
        const result = await pool.query<OrderRow>("SELECT id, user_id, product_id, amount, status, payment_transaction_id, created_at FROM orders WHERE user_id = $1 ORDER BY created_at, id", [call.request.userId])
        callback(null, { orders: result.rows.map(orderProto) })
      } catch (error) {
        console.error("Order query failed", error)
        callback({ code: grpc.status.INTERNAL, message: "Order query failed" })
      }
    }
  })
  await new Promise<void>((resolve, reject) => {
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (error) => error ? reject(error) : resolve())
  })
  return server
}

export async function stopOrderQueryServer(server: grpc.Server): Promise<void> {
  await new Promise<void>((resolve) => server.tryShutdown(() => resolve()))
}
