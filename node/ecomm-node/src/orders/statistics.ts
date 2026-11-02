import type pg from "pg"

interface OrderCreated { orderId: string; userId: string; amount: number }

/** Apply each order once, even if Kafka redelivers it after a commit failure. */
export async function updateStatistics(pool: pg.Pool, event: OrderCreated): Promise<void> {
  const client = await pool.connect()
  try {
    await client.query("BEGIN")
    const inserted = await client.query("INSERT INTO processed_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING", [event.orderId])
    if (inserted.rowCount) {
      await client.query(`INSERT INTO user_order_statistics (user_id, total_orders, total_amount, last_order_at)
        VALUES ($1, 1, $2, NOW()) ON CONFLICT (user_id) DO UPDATE SET
        total_orders = user_order_statistics.total_orders + 1,
        total_amount = user_order_statistics.total_amount + EXCLUDED.total_amount,
        last_order_at = EXCLUDED.last_order_at`, [event.userId, event.amount])
    }
    await client.query("COMMIT")
  } catch (error) {
    await client.query("ROLLBACK")
    throw error
  } finally {
    client.release()
  }
}
