package com.trendyol.stove.examples.ecomm.e2e.setup

import arrow.core.getOrElse
import arrow.core.some
import com.trendyol.stove.examples.ecomm.e2e.tests.ConfirmationJob
import com.trendyol.stove.reporting.Reports
import com.trendyol.stove.reporting.SystemSnapshot
import com.trendyol.stove.system.Stove
import com.trendyol.stove.system.ValidationDsl
import com.trendyol.stove.system.WithDsl
import com.trendyol.stove.system.abstractions.PluggedSystem
import com.trendyol.stove.system.abstractions.SystemNotRegisteredException

/** Reports successful JVM scheduler executions observed by its listener. */
class ConfirmationSystem(
  override val stove: Stove,
  private val await: suspend (ConfirmationJob) -> Unit,
  private val executions: () -> List<Map<String, Any?>>,
  val validateBean: suspend ValidationDsl.(String, String) -> Unit
) : PluggedSystem, Reports {
  override val reportSystemName = "DbScheduler"

  suspend fun shouldBeExecuted(job: ConfirmationJob) {
    report(action = "Assert confirmation task executed", expected = "Successful confirmation for ${job.orderId}".some()) {
      await(job)
    }
  }

  override fun snapshot() = SystemSnapshot(system = reportSystemName, state = mapOf("executions" to executions()), summary = "Observed ${executions().size} execution(s)")
  override fun close() = Unit
}

fun WithDsl.confirmations(await: suspend (ConfirmationJob) -> Unit, executions: () -> List<Map<String, Any?>>, validateBean: suspend ValidationDsl.(String, String) -> Unit): Stove =
  stove.getOrRegister(ConfirmationSystem(stove, await, executions, validateBean)).let { stove }

suspend fun ValidationDsl.confirmations(validation: suspend ConfirmationSystem.() -> Unit) =
  validation(stove.getOrNone<ConfirmationSystem>().getOrElse { throw SystemNotRegisteredException(ConfirmationSystem::class) })
