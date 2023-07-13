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

package alluxio.master;

import alluxio.concurrent.jsr.ForkJoinPool;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Forwarder over ExecutorService interface for exposing internal queue length.
 */
public class AlluxioExecutorService implements ExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioExecutorService.class);

  private ExecutorService mExecutor;
  private final Counter mRpcTracker;

  /**
   * Creates Alluxio ExecutorService wrapper.
   *
   * @param executor underlying executor
   */
  public AlluxioExecutorService(ExecutorService executor) {
    mExecutor = executor;
    mRpcTracker = null;
  }

  /**
   * Creates Alluxio ExecutorService wrapper.
   *
   * @param executor underlying executor
   * @param counter the counter to track active operations
   */
  public AlluxioExecutorService(ExecutorService executor, Counter counter) {
    mExecutor = executor;
    mRpcTracker = counter;
  }

  /**
   * @return the current RPC queue size
   */
  public long getRpcQueueLength() {
    if (mExecutor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor) mExecutor).getQueue().size();
    } else if (mExecutor instanceof ForkJoinPool) {
      return ((ForkJoinPool) mExecutor).getQueuedSubmissionCount();
    } else {
      throw new IllegalArgumentException(
          String.format("Not supported internal executor: %s", mExecutor.getClass().getName()));
    }
  }

  /**
   * @return the current RPC active thread count
   */
  public long getActiveCount() {
    if (mExecutor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor) mExecutor).getActiveCount();
    } else if (mExecutor instanceof ForkJoinPool) {
      return ((ForkJoinPool) mExecutor).getActiveThreadCount();
    } else {
      throw new IllegalArgumentException(
          String.format("Not supported internal executor: %s", mExecutor.getClass().getName()));
    }
  }

  /**
   * @return the current RPC thread pool size
   */
  public long getPoolSize() {
    if (mExecutor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor) mExecutor).getPoolSize();
    } else if (mExecutor instanceof ForkJoinPool) {
      return ((ForkJoinPool) mExecutor).getPoolSize();
    } else {
      throw new IllegalArgumentException(
          String.format("Not supported internal executor: %s", mExecutor.getClass().getName()));
    }
  }

  @Override
  public void shutdown() {
    if (mRpcTracker != null) {
      long activeRpcCount = mRpcTracker.getCount();
      if (activeRpcCount > 0) {
        LOG.warn("{} operations have not completed", activeRpcCount);
      }
    }
    mExecutor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    if (mRpcTracker != null) {
      long activeRpcCount = mRpcTracker.getCount();
      if (activeRpcCount > 0) {
        LOG.warn("{} operations have not completed", activeRpcCount);
      }
    }
    return mExecutor.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return mExecutor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return mExecutor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return mExecutor.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    if (mRpcTracker != null) {
      mRpcTracker.inc();
      LOG.trace("Inc from rpc server in submit(Callable)");
    }
    try {
      return mExecutor.submit(task);
    } finally {
      if (mRpcTracker != null) {
        mRpcTracker.dec();
      }
    }
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    if (mRpcTracker != null) {
      mRpcTracker.inc();
      LOG.trace("Inc from rpc server in submit(Runnable,T)");
    }
    try {
      return mExecutor.submit(task, result);
    } finally {
      if (mRpcTracker != null) {
        mRpcTracker.dec();
      }
    }
  }

  @Override
  public Future<?> submit(Runnable task) {
    if (mRpcTracker != null) {
      mRpcTracker.inc();
      LOG.trace("Inc from rpc server in submit(Runnable)");
    }
    try {
      return mExecutor.submit(task);
    } finally {
      if (mRpcTracker != null) {
        mRpcTracker.dec();
      }
    }
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    if (mRpcTracker != null) {
      mRpcTracker.inc();
      LOG.trace("Inc from rpc server in invokeAll(Collection)");
    }
    try {
      return mExecutor.invokeAll(tasks);
    } finally {
      if (mRpcTracker != null) {
        mRpcTracker.dec();
      }
    }
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit) throws InterruptedException {
    if (mRpcTracker != null) {
      mRpcTracker.inc();
      LOG.trace("Inc from rpc server in invokeAll(Collection,long,TimeUnit)");
    }
    try {
      return mExecutor.invokeAll(tasks, timeout, unit);
    } finally {
      if (mRpcTracker != null) {
        mRpcTracker.dec();
      }
    }
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
    // Not used. Also the active counter is hard, so we do not support it.
    throw new UnsupportedOperationException("invokeAny(Collection) is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
    // Not used. Also the active counter is hard, so we do not support it.
    throw new UnsupportedOperationException(
        "invokeAny(Collection,long,TimeUnit) is not supported");
  }

  @Override
  public void execute(Runnable command) {
    if (mRpcTracker != null) {
      mRpcTracker.inc();
      LOG.trace("Inc from rpc server in execute(Runnable)");
    }
    try {
      mExecutor.execute(command);
    } finally {
      if (mRpcTracker != null) {
        mRpcTracker.dec();
      }
    }
  }
}
