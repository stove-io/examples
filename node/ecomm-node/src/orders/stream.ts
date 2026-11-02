import type { ServerResponse } from "node:http"
import type pg from "pg"

/** Read the customer's persisted orders before starting the response so errors remain visible. */
export async function streamOrders(pool: pg.Pool, userId: string, response: ServerResponse): Promise<void> {
  const result = await pool.query(
    "SELECT id, user_id, product_id, amount, status FROM orders WHERE user_id = $1 ORDER BY created_at, id",
    [userId]
  )
  response.writeHead(200, { "content-type": "application/x-ndjson" })
  for (const row of result.rows) {
    response.write(JSON.stringify({ orderId: row.id, userId: row.user_id, productId: row.product_id, amount: Number(row.amount), status: row.status }) + "\n")
  }
  response.end()
}
