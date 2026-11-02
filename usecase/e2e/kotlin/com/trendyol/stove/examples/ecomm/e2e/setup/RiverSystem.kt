package com.trendyol.stove.examples.ecomm.e2e.setup

import arrow.core.getOrElse
import arrow.core.some
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.trendyol.stove.rdbms.RelationalDatabaseExposedConfiguration
import com.trendyol.stove.reporting.SystemSnapshot
import com.trendyol.stove.reporting.Reports
import com.trendyol.stove.system.*
import com.trendyol.stove.system.abstractions.AfterRunAware
import com.trendyol.stove.system.abstractions.PluggedSystem
import com.trendyol.stove.system.abstractions.SystemNotRegisteredException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.postgresql.PGConnection
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Observes committed River state changes without polling application state. */
class RiverSystem(override val stove: Stove, private val database: () -> RelationalDatabaseExposedConfiguration) :
  PluggedSystem, AfterRunAware, Reports {
  override val reportSystemName: String = "River"
  private val mapper: ObjectMapper = jacksonObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  private val identifier = "stove_river_" + UUID.randomUUID().toString().replace("-", "")
  private val observed = ConcurrentHashMap<Long, Map<String, Any?>>()
  @Volatile private var installed = false
  private fun connect(): Connection = database().let { DriverManager.getConnection(it.jdbcUrl, it.username, it.password) }

  override suspend fun afterRun() = withContext(Dispatchers.IO) {
    connect().use { db -> db.createStatement().use { statement ->
      statement.execute("""
        CREATE FUNCTION ${identifier}() RETURNS trigger AS ${'$'}${'$'}
        BEGIN
          IF NEW.state IN ('completed', 'discarded', 'cancelled') AND OLD.state IS DISTINCT FROM NEW.state THEN
            PERFORM pg_notify('$identifier', NEW.id::text);
          END IF;
          RETURN NEW;
        END;
        ${'$'}${'$'} LANGUAGE plpgsql;
        CREATE TRIGGER $identifier AFTER UPDATE OF state ON river_job
          FOR EACH ROW EXECUTE FUNCTION ${identifier}();
      """.trimIndent())
    } }
    installed = true
    Unit
  }

  suspend fun <T : Any> shouldBeExecuted(kind: String, type: Class<T>, timeout: Duration = 15.seconds, condition: (T) -> Boolean): RiverSystem {
    report(action = "Assert River job executed: $kind", expected = "Matching job completed successfully".some(), metadata = mapOf("kind" to kind, "timeout" to timeout.toString())) {
      withContext(Dispatchers.IO) {
        connect().use { db ->
          db.createStatement().use { it.execute("LISTEN $identifier") }
          val listener = db.unwrap(PGConnection::class.java)
          val deadline = System.nanoTime() + timeout.inWholeNanoseconds
          while (true) {
            db.prepareStatement("SELECT id, state::text, args::text, errors::text FROM river_job WHERE kind = ? AND state IN ('completed','discarded','cancelled') ORDER BY id DESC").use { query ->
              query.setString(1, kind)
              query.executeQuery().use { rows ->
                while (rows.next()) {
                  val id = rows.getLong(1)
                  val state = rows.getString(2)
                  val errors = rows.getString(4)
                  observed[id] = mapOf("id" to id, "kind" to kind, "state" to state, "errors" to errors)
                  val args = mapper.readValue(rows.getString(3), type)
                  if (condition(args)) {
                    check(state == "completed") { "River job $kind ($id) $state: $errors" }
                    return@withContext
                  }
                }
              }
            }
            val remaining = deadline - System.nanoTime()
            check(remaining > 0) { "Timed out waiting for River job $kind to complete" }
            listener.getNotifications((remaining / 1_000_000).coerceIn(1, Int.MAX_VALUE.toLong()).toInt())
          }
        }
      }
    }
    return this
  }

  override fun snapshot(): SystemSnapshot {
    val jobs = observed.values.toList()
    return SystemSnapshot(system = reportSystemName, state = mapOf("observedJobs" to jobs), summary = "Observed ${jobs.size} finished River job(s)")
  }

  override fun close() {
    if (!installed) return
    connect().use { db -> db.createStatement().use { statement ->
      statement.execute("DROP TRIGGER IF EXISTS $identifier ON river_job; DROP FUNCTION IF EXISTS ${identifier}()")
    } }
    installed = false
  }
}

fun WithDsl.river(database: () -> RelationalDatabaseExposedConfiguration): Stove =
  stove.getOrRegister(RiverSystem(stove, database)).let { stove }

suspend fun ValidationDsl.river(validation: suspend RiverSystem.() -> Unit) =
  validation(stove.getOrNone<RiverSystem>().getOrElse { throw SystemNotRegisteredException(RiverSystem::class) })

suspend inline fun <reified T : Any> RiverSystem.shouldBeExecuted(kind: String, timeout: Duration = 15.seconds, noinline condition: (T) -> Boolean): RiverSystem =
  shouldBeExecuted(kind, T::class.java, timeout, condition)
