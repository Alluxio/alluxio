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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Forwarder over ExecutorService interface for exposing internal queue length.
 */
public class AlluxioExecutorService implements ExecutorService {
  private ExecutorService mExecutor;

  /**
   * Creates Alluxio ExecutorService wrapper.
   *
   * @param executor underlying executor
   */
  public AlluxioExecutorService(ExecutorService executor) {
    mExecutor = executor;
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
    mExecutor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
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
    return mExecutor.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return mExecutor.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return mExecutor.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return mExecutor.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit) throws InterruptedException {
    return mExecutor.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return mExecutor.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    mExecutor.execute(command);
  }
}
