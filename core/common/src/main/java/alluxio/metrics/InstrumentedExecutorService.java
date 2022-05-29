/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.metrics;

import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.logging.SamplingLogger;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A wrapper around {@link com.codahale.metrics.InstrumentedExecutorService}
 * that allows the metrics to be reset by {@link MetricsSystem#resetAllMetrics()}.
 * Additional it tracks in a histogram called name.active.tasks
 * the number of active tasks (queued or running) each
 * time a new task is added to the executor. This histogram is additionally
 * tracks the maximum overall number active tasks at any time.
 */
public class InstrumentedExecutorService implements ExecutorService {
  private final Logger mSamplingLog =
      new SamplingLogger(LoggerFactory.getLogger(InstrumentedExecutorService.class),
          ConfigurationUtils.defaults().getMs(PropertyKey.METRICS_EXECUTOR_TASK_WARN_FREQUENCY));

  private com.codahale.metrics
      .InstrumentedExecutorService mDelegate;
  private final String mName;
  private final MetricRegistry mRegistry;
  private final ExecutorService mExecutorService;
  private final MetricRegistry.MetricSupplier<Histogram> mSupplier =
      () -> new Histogram(new MaxReservoir(new ExponentiallyDecayingReservoir()));
  private Meter mSubmitted;
  private Meter mCompleted;
  private Histogram mHist;

  /**
   * @param executorService the executor service to instrument
   * @param registry the metric registry
   * @param name the name that will be used for the associated metrics
   */
  public InstrumentedExecutorService(
      ExecutorService executorService, MetricRegistry registry, String name) {
    mName = name;
    mRegistry = registry;
    mExecutorService = executorService;
    if (executorService instanceof ThreadPoolExecutor) {
      BlockingQueue<Runnable> queue = ((ThreadPoolExecutor) executorService).getQueue();
      MetricsSystem.registerCachedGaugeIfAbsent(
          MetricRegistry.name(mName, "queueSize"),
          new CachedGauge<Integer>(1, TimeUnit.SECONDS) {
            @Override
            protected Integer loadValue() {
              return queue.size();
            }
          });
    }
    reset();
  }

  /**
   * Resets the metrics monitored about the executor service.
   * This is only called by {@link MetricsSystem#resetAllMetrics()}
   * after all metrics have already been cleared, then this method
   * will update the pointers of this object to the new metrics.
   */
  protected void reset() {
    mDelegate = new com.codahale.metrics.InstrumentedExecutorService(
        mExecutorService, mRegistry, mName);
    mSubmitted = mRegistry.meter(MetricRegistry.name(mName, "submitted"));
    mCompleted = mRegistry.meter(MetricRegistry.name(mName, "completed"));
    String histName = MetricRegistry.name(mName, "activeTaskQueue");
    mRegistry.remove(histName);
    mHist = mRegistry.histogram(histName, mSupplier);
  }

  private void addedTasks(int count) {
    long activeCount = mSubmitted.getCount() - mCompleted.getCount() + count;
    mHist.update(activeCount);
    if (activeCount >= ConfigurationUtils.defaults()
        .getInt(PropertyKey.METRICS_EXECUTOR_TASK_WARN_SIZE)) {
      mSamplingLog.warn("Number of active tasks (queued and running) for executor {} is {}",
          mName, activeCount);
    }
  }

  @Override
  public void execute(@Nonnull Runnable runnable) {
    addedTasks(1);
    mDelegate.execute(runnable);
  }

  @Override
  public Future<?> submit(@Nonnull Runnable runnable) {
    addedTasks(1);
    return mDelegate.submit(runnable);
  }

  @Override
  public <T> Future<T> submit(@Nonnull Runnable runnable, T result) {
    addedTasks(1);
    return mDelegate.submit(runnable, result);
  }

  @Override
  public <T> Future<T> submit(@Nonnull Callable<T> task) {
    addedTasks(1);
    return mDelegate.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      @Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException {
    addedTasks(tasks.size());
    return mDelegate.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      @Nonnull Collection<? extends Callable<T>> tasks, long timeout,
      @Nonnull TimeUnit unit) throws InterruptedException {
    addedTasks(tasks.size());
    return mDelegate.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(
      @Nonnull Collection<? extends Callable<T>> tasks)
      throws ExecutionException, InterruptedException {
    addedTasks(tasks.size());
    return mDelegate.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(
      @Nonnull Collection<? extends Callable<T>> tasks,
      long timeout, @Nonnull TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {
    addedTasks(tasks.size());
    return mDelegate.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void shutdown() {
    mDelegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return mDelegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return mDelegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return mDelegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long l, @Nonnull TimeUnit timeUnit) throws InterruptedException {
    return mDelegate.awaitTermination(l, timeUnit);
  }
}
