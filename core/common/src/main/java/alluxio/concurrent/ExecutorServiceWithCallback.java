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

package alluxio.concurrent;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A wrapper over an {@link ExecutorService}. When a task is executed in this executor, the runnable
 * provided is executed in the same thread.
 */
public class ExecutorServiceWithCallback implements ExecutorService {
  private final ExecutorService mExecutor;
  private final Runnable mRunnable;

  /**
   * Creates an instance of {@link ExecutorServiceWithCallback}.
   *
   * @param executor the {@link ExecutorService}
   * @param runnable the callback
   */
  public ExecutorServiceWithCallback(ExecutorService executor, Runnable runnable) {
    Preconditions.checkNotNull(executor);
    Preconditions.checkNotNull(runnable);
    mExecutor = executor;
    mRunnable = runnable;
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
  public <T> Future<T> submit(final Callable<T> task) {
    return mExecutor.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        T t;
        try {
          t = task.call();
        } finally {
          mRunnable.run();
        }
        return t;
      }
    });
  }

  @Override
  public <T> Future<T> submit(final Runnable task, T result) {
    return mExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          task.run();
        } finally {
          mRunnable.run();
        }
      }
    }, result);
  }

  @Override
  public Future<?> submit(final Runnable task) {
    return mExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          task.run();
        } finally {
          mRunnable.run();
        }
      }
    });
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new NotImplementedException("invoke* methods are not implemented.");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit) throws InterruptedException {
    throw new NotImplementedException("invoke* methods are not implemented.");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new NotImplementedException("invoke* methods are not implemented.");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new NotImplementedException("invoke* methods are not implemented.");
  }

  @Override
  public void execute(Runnable command) {
    try {
      command.run();
    } finally {
      mRunnable.run();
    }
  }
}
