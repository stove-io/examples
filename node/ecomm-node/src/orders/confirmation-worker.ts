import { PgDriver } from "@riverqueue/driver-pg"
import { createMigrator } from "@riverqueue/migrate"
import { Client, Workers, defineJob } from "riverqueue"
import { z } from "zod"
import type pg from "pg"

const confirmationSchema = z.object({
    orderId: z.string(), userId: z.string(), productId: z.string(), amount: z.number(),
    correlation: z.record(z.string(), z.string())
})
const confirmation = defineJob({ kind: "order_confirmation", schema: confirmationSchema })

type Confirmation = z.infer<typeof confirmationSchema>
type Publish = (topic: string, key: string, event: object, correlation: Record<string, string>) => Promise<void>

/** Durable confirmation jobs use the same PostgreSQL database as checkout. */
export async function startConfirmationWorker(pool: pg.Pool, publish: Publish) {
  const driver = new PgDriver(pool)
  await createMigrator(driver).migrateUp()
  const workers = new Workers().add(confirmation, async ({ job }) => {
    const { orderId, userId, productId, amount, correlation } = job.args
    // The example simulates sending an email; the event gives tests deterministic evidence of the work.
    console.log(`Order confirmation for ${userId}: ${orderId}`)
    await publish("order.confirmation.sent", orderId, { orderId, userId, productId, amount }, correlation)
  })
  const client = new Client(driver, { queues: { default: { maxWorkers: 4 } }, workers })
  const run = await client.start()
  return {
    schedule: async (args: Confirmation) => { await client.insert(confirmation, args) },
    close: async () => { await run.stop({ timeout: { seconds: 5 } }); await run.completed }
  }
}
