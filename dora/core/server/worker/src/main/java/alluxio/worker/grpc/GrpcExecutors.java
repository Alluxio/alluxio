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

package alluxio.worker.grpc;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Executors for gRPC block server.
 */
@ThreadSafe
public final class GrpcExecutors {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcExecutors.class);
  private static final long THREAD_STOP_MS = Constants.SECOND_MS * 10;
  private static final int THREADS_MIN = 4;

  // Used by BlockWorkerClientServiceHandler.readBlock() by DataReader threads.
  // Mainly used by distributed load and obsolete clients.
  // The thread pool queue is always empty.
  private static final ThreadPoolExecutor READER_THREAD_POOL_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN, Configuration.getInt(
          PropertyKey.WORKER_NETWORK_GRPC_READER_THREADS_MAX), THREAD_STOP_MS,
          TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          ThreadFactoryUtils.build("GrpcDataReaderExecutor-%d", true));
  public static final ExecutorService READER_EXECUTOR =
      new ImpersonateThreadPoolExecutor(READER_THREAD_POOL_EXECUTOR, true);

  // Used for replying data to the client in FileReadHandler.
  // The thread pool has a small queue of a constant size.
  private static final ThreadPoolExecutor SERIALIZED_THREAD_POOL_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_GRPC_READER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(32),
          ThreadFactoryUtils.build("GrpcDataReaderSerializedExecutor-%d", true),
          new ThreadPoolExecutor.CallerRunsPolicy());
  public static final ExecutorService READER_SERIALIZED_RUNNER_EXECUTOR =
      new ImpersonateThreadPoolExecutor(SERIALIZED_THREAD_POOL_EXECUTOR, true);

  // Mainly used by distributed mv/cp and obsolete clients.
  // The queue is always empty.
  private static final ThreadPoolExecutor WRITE_THREAD_POOL_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN, Configuration.getInt(
          PropertyKey.WORKER_NETWORK_GRPC_WRITER_THREADS_MAX), THREAD_STOP_MS,
          TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          ThreadFactoryUtils.build("GrpcDataWriterExecutor-%d", true));
  public static final ExecutorService WRITER_EXECUTOR =
          new ImpersonateThreadPoolExecutor(WRITE_THREAD_POOL_EXECUTOR, true);

  static {
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_READER_THREAD_ACTIVE_COUNT.getName()),
        READER_THREAD_POOL_EXECUTOR::getActiveCount);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_READER_THREAD_CURRENT_COUNT.getName()),
        READER_THREAD_POOL_EXECUTOR::getPoolSize);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_READER_THREAD_MAX_COUNT.getName()),
        READER_THREAD_POOL_EXECUTOR::getMaximumPoolSize);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_READER_COMPLETED_TASK_COUNT.getName()),
        READER_THREAD_POOL_EXECUTOR::getCompletedTaskCount);

    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_SERIALIZED_THREAD_ACTIVE_COUNT.getName()),
        SERIALIZED_THREAD_POOL_EXECUTOR::getActiveCount);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_SERIALIZED_THREAD_CURRENT_COUNT.getName()),
        SERIALIZED_THREAD_POOL_EXECUTOR::getPoolSize);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_SERIALIZED_THREAD_MAX_COUNT.getName()),
        SERIALIZED_THREAD_POOL_EXECUTOR::getMaximumPoolSize);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_SERIALIZED_COMPLETED_TASK_COUNT.getName()),
        SERIALIZED_THREAD_POOL_EXECUTOR::getCompletedTaskCount);

    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_WRITER_THREAD_ACTIVE_COUNT.getName()),
        WRITE_THREAD_POOL_EXECUTOR::getActiveCount);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_WRITER_THREAD_CURRENT_COUNT.getName()),
        WRITE_THREAD_POOL_EXECUTOR::getPoolSize);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_WRITER_THREAD_MAX_COUNT.getName()),
        WRITE_THREAD_POOL_EXECUTOR::getMaximumPoolSize);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
        MetricKey.WORKER_BLOCK_WRITER_COMPLETED_TASK_COUNT.getName()),
        WRITE_THREAD_POOL_EXECUTOR::getCompletedTaskCount);
  }

  /**
   * Private constructor.
   */
  private GrpcExecutors() {}

  /**
   * This executor passes impersonation information to the real worker thread.
   * The proxy user is tracked by {@link AuthenticatedClientUser#sUserThreadLocal}.
   * This executor delegates operations to the underlying executor while setting the
   * ThreadLocal context for execution.
   * */
  private static class ImpersonateThreadPoolExecutor extends AbstractExecutorService {
    private final ExecutorService mDelegate;
    private final boolean mTracked;
    private Counter mClientCounter;

    public ImpersonateThreadPoolExecutor(ExecutorService service, boolean tracked) {
      mDelegate = service;
      mTracked = tracked;
      if (tracked) {
        mClientCounter = MetricsSystem.counter(MetricKey.WORKER_ACTIVE_OPERATIONS.getName());
      }
    }

    @Override
    public void execute(final Runnable command) {
      // If there's no impersonation, proxyUser is just null
      User proxyUser = AuthenticatedClientUser.getOrNull();
      mDelegate.execute(() -> {
        if (mTracked) {
          mClientCounter.inc();
        }
        try {
//          SleepUtils.sleepMs(1000);
          AuthenticatedClientUser.set(proxyUser);
          command.run();
        } finally {
          if (mTracked) {
            mClientCounter.dec();
          }
          AuthenticatedClientUser.remove();
        }
      });
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      // If there's no impersonation, proxyUser is just null
      User proxyUser = AuthenticatedClientUser.getOrNull();
      return mDelegate.submit(() -> {
        if (mTracked) {
          mClientCounter.inc();
        }
        try {
//          SleepUtils.sleepMs(1000);
          AuthenticatedClientUser.set(proxyUser);
          return task.call();
        } finally {
          if (mTracked) {
            mClientCounter.dec();
          }
          AuthenticatedClientUser.remove();
        }
      });
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      // If there's no impersonation, proxyUser is just null
      User proxyUser = AuthenticatedClientUser.getOrNull();
      return mDelegate.submit(() -> {
        if (mTracked) {
          mClientCounter.inc();
        }
        try {
//          SleepUtils.sleepMs(1000);
          AuthenticatedClientUser.set(proxyUser);
          task.run();
        } finally {
          if (mTracked) {
            mClientCounter.dec();
          }
          AuthenticatedClientUser.remove();
        }
      }, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
      // If there's no impersonation, proxyUser is just null
      User proxyUser = AuthenticatedClientUser.getOrNull();
      return mDelegate.submit(() -> {
        if (mTracked) {
          mClientCounter.inc();
        }
        try {
//          SleepUtils.sleepMs(1000);
          AuthenticatedClientUser.set(proxyUser);
          task.run();
        } finally {
          if (mTracked) {
            mClientCounter.dec();
          }
          AuthenticatedClientUser.remove();
        }
      });
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      // Not used. Also the active counter is hard, so we do not support it.
      throw new UnsupportedOperationException("invokeAll(Collection) is not supported");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
      // Not used. Also the active counter is hard, so we do not support it.
      throw new UnsupportedOperationException(
          "invokeAll(Collection,long,TimeUnit) is not supported");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
      // Not used. Also the active counter is hard, so we do not support it.
      throw new UnsupportedOperationException("invokeAny(Callable) is not supported");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
      // Not used. Also the active counter is hard, so we do not support it.
      throw new UnsupportedOperationException(
          "invokeAny(Callable,long,TimeUnit) is not supported");
    }

    @Override
    public void shutdown() {
      if (mTracked) {
        long operationCount = mClientCounter.getCount();
        if (operationCount > 0) {
          LOG.warn("{} operations have not completed at shutdown()", operationCount);
        }
      }
      mDelegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      if (mTracked) {
        long operationCount = mClientCounter.getCount();
        if (operationCount > 0) {
          LOG.warn("{} operations have not completed at shutdownNow()", operationCount);
        }
      }
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
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return mDelegate.awaitTermination(timeout, unit);
    }
  }
}
