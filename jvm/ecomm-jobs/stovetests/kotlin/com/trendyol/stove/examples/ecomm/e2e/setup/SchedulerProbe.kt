package com.trendyol.stove.examples.ecomm.e2e.setup

import com.github.kagkarlsson.scheduler.event.AbstractSchedulerListener
import com.github.kagkarlsson.scheduler.task.ExecutionComplete
import com.trendyol.stove.examples.ecomm.e2e.tests.ConfirmationJob
import com.trendyol.stove.examples.ecomm.jobs.Confirmation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentLinkedQueue

/** Listener callbacks wake the assertion after the scheduler finishes the task. */
class SchedulerProbe : AbstractSchedulerListener() {
  private val completed = ConcurrentLinkedQueue<ExecutionComplete>()
  private val changed = Channel<Unit>(Channel.CONFLATED)

  override fun onExecutionComplete(executionComplete: ExecutionComplete) {
    completed.add(executionComplete)
    changed.trySend(Unit)
  }

  suspend fun await(job: ConfirmationJob) = withTimeout(15_000) {
    while (true) {
      val match = completed.firstOrNull { execution ->
        val payload = execution.execution.taskInstance.data as? Confirmation
        payload != null && payload.orderId() == job.orderId && payload.userId() == job.userId &&
          payload.productId() == job.productId && payload.amount() == job.amount
      }
      if (match != null) {
        check(match.result == ExecutionComplete.Result.OK) { "Confirmation failed: ${match.cause.orElse(null)}" }
        return@withTimeout
      }
      changed.receive()
    }
  }

  fun snapshot(): List<Map<String, Any?>> = completed.map {
    mapOf("task" to it.execution.taskInstance.taskName, "id" to it.execution.taskInstance.id,
      "result" to it.result.toString(), "error" to it.cause.orElse(null)?.message)
  }
}
