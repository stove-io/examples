import type { IncomingMessage, ServerResponse } from "node:http"
import { context, propagation, SpanKind, trace } from "@opentelemetry/api"

export function writeJson(response: ServerResponse, status: number, body: unknown) {
  response.writeHead(status, { "content-type": "application/json" })
  response.end(JSON.stringify(body))
}

export function readBody(request: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    request.on("data", (chunk) => chunks.push(chunk))
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")))
    request.on("error", reject)
  })
}

export async function inSpan<T>(request: IncomingMessage, name: string, action: () => Promise<T>): Promise<T> {
  const tracer = trace.getTracer("ecomm-node")
  const parent = propagation.extract(context.active(), request.headers)
  const span = tracer.startSpan(name, { kind: SpanKind.SERVER }, parent)
  try {
    return await context.with(trace.setSpan(parent, span), action)
  } finally {
    span.end()
  }
}
