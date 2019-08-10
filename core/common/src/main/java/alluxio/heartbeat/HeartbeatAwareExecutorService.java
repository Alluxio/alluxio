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

package alluxio.heartbeat;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to safely wrap java {@link ExecutorService} with functionality to let
 * heartbeat threads know if shutdown in progress.
 */
public class HeartbeatAwareExecutorService implements ExecutorService {
  /** Underlying executor. */
  private final ExecutorService mExecutor;
  /** Shutdown tracker for the executor. */
  private final AtomicBoolean mShutdownTracker;

  /**
   * Creates new instance with given underlying executor.
   *
   * @param executorService underlying {@link ExecutorService}
   */
  public HeartbeatAwareExecutorService(ExecutorService executorService) {
    mExecutor = executorService;
    mShutdownTracker = new AtomicBoolean(false);
  }

  @Override
  public void shutdown() {
    mShutdownTracker.set(true);
    mExecutor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    mShutdownTracker.set(true);
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
    setShutdownTracker(task);
    return mExecutor.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    setShutdownTracker(task);
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
    return mExecutor.invokeAny(tasks);
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

  private void setShutdownTracker(Runnable task) {
    if (task instanceof HeartbeatThread) {
      ((HeartbeatThread) task).setShutdownTracker(mShutdownTracker);
    }
  }
}
