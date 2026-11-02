package com.trendyol.stove.examples.ecomm.jobs;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.sql.DataSource;

/** Persistent confirmation work shared by the JVM commerce applications. */
public final class ConfirmationScheduler implements AutoCloseable {
  private final Task<Confirmation> task;
  private final Scheduler scheduler;
  private final AtomicBoolean closed = new AtomicBoolean();

  public ConfirmationScheduler(DataSource dataSource, Consumer<Confirmation> publish) {
    task = Tasks.oneTime("order-confirmation", Confirmation.class)
        .execute((instance, context) -> publish.accept(instance.getData()));
    scheduler = Scheduler.create(dataSource, task)
        .pollingInterval(Duration.ofMillis(100))
        .enableImmediateExecution()
        .threads(2)
        .build();
    scheduler.start();
  }

  public void schedule(Confirmation confirmation) {
    scheduler.scheduleIfNotExists(task.instance(confirmation.orderId(), confirmation), Instant.now());
  }

  public void addListener(SchedulerListener listener) {
    scheduler.registerSchedulerListener(listener);
  }

  @Override public void close() { if (closed.compareAndSet(false, true)) scheduler.stop(); }
}
