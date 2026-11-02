import { randomUUID } from "node:crypto"
import { fileURLToPath } from "node:url"
import * as grpc from "@grpc/grpc-js"
import * as protoLoader from "@grpc/proto-loader"
import type pg from "pg"

export interface CreateOrder { userId: string; productId: string; amount: number }
export interface OrderResponse extends CreateOrder { orderId: string; status: string }
interface FraudResponse { isFraudulent: boolean; reason: string }
interface InventoryResponse { available: boolean }
interface PaymentResponse { success: boolean; transactionId?: string; errorMessage?: string }
type FraudClient = grpc.Client & { checkFraud(request: object, options: grpc.CallOptions, callback: (error: grpc.ServiceError | null, response: FraudResponse) => void): void }
type Publish = (topic: string, key: string, event: object, correlation: Record<string, string>) => Promise<void>

export class CheckoutError extends Error {
  constructor(readonly status: number, readonly errorCode: string, message: string) { super(message) }
}

/** Owns the external fraud channel and orchestrates the synchronous checkout path. */
export class CheckoutService {
  private readonly fraud: FraudClient
  constructor(private readonly pool: pg.Pool, private readonly inventoryUrl: string, private readonly paymentUrl: string, fraudTarget: string, private readonly publish: Publish, private readonly scheduleConfirmation: (order: OrderResponse, correlation: Record<string, string>) => Promise<void>) {
    const path = fileURLToPath(new URL("../proto/fraud_detection.proto", import.meta.url))
    const definition = protoLoader.loadSync(path, { keepCase: false, defaults: true })
    const loaded = grpc.loadPackageDefinition(definition) as unknown as { frauddetection: { FraudDetectionService: new (address: string, credentials: grpc.ChannelCredentials) => FraudClient } }
    this.fraud = new loaded.frauddetection.FraudDetectionService(fraudTarget, grpc.credentials.createInsecure())
  }

  async create(request: CreateOrder, correlation: Record<string, string>): Promise<OrderResponse> {
    const orderId = randomUUID()
    const fraud = await new Promise<FraudResponse>((resolve, reject) => {
      this.fraud.checkFraud({ orderId, ...request }, { deadline: Date.now() + 5000 }, (error, result) => error ? reject(error) : resolve(result))
    })
    if (fraud.isFraudulent) throw new CheckoutError(500, "INTERNAL_ERROR", "Order flagged as fraudulent")
    const inventory = await this.json<InventoryResponse>(`${this.inventoryUrl}/inventory/${encodeURIComponent(request.productId)}`)
    if (!inventory.available) throw new CheckoutError(409, "INVENTORY_NOT_AVAILABLE", `Inventory not available for product: ${request.productId}`)
    const payment = await this.json<PaymentResponse>(`${this.paymentUrl}/payments/charge`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ userId: request.userId, amount: request.amount })
    })
    if (!payment.success) throw new CheckoutError(502, "PAYMENT_FAILED", `Payment failed: ${payment.errorMessage ?? "Unknown error"}`)
    if (!payment.transactionId) throw new Error("Payment response has no transaction ID")
    const order: OrderResponse = { orderId, ...request, status: "CONFIRMED" }
    await this.pool.query("INSERT INTO orders (id, user_id, product_id, amount, status, payment_transaction_id) VALUES ($1, $2, $3, $4, $5, $6)",
      [orderId, request.userId, request.productId, request.amount, order.status, payment.transactionId])
    await this.scheduleConfirmation(order, correlation)
    const createdAt = new Date().toISOString()
    await this.publish("showcase.orders.created", orderId, { orderId, ...request, createdAt }, correlation)
    await this.publish("showcase.payments.processed", orderId, { orderId, transactionId: payment.transactionId, amount: request.amount, success: true, createdAt }, correlation)
    return order
  }

  async find(id: string): Promise<OrderResponse | null> {
    const result = await this.pool.query("SELECT id, user_id, product_id, amount, status FROM orders WHERE id = $1", [id])
    const row = result.rows[0]
    return row ? { orderId: row.id, userId: row.user_id, productId: row.product_id, amount: Number(row.amount), status: row.status } : null
  }

  close(): void { this.fraud.close() }

  private async json<T>(url: string, options: RequestInit = {}): Promise<T> {
    const result = await fetch(url, { ...options, signal: AbortSignal.timeout(5000) })
    if (!result.ok) throw new Error(`External service returned HTTP ${result.status}`)
    return await result.json() as T
  }
}
