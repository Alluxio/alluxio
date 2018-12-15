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

package alluxio.util.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A controllable {@link ScheduledExecutorService} that supports jump to a future time, runs
 * next/all pending commands. This class is designed for test purpose.
 */
public class ControllableScheduler implements ScheduledExecutorService {
  private final DeltaQueue<ScheduledTask<?>> mDeltaQueue = new DeltaQueue<>();

  /**
   * Jumps to a future time by a given duration. All the commands/tasks scheduled
   * for execution during this period will be executed. When this function returns,
   * the executor will be idle.
   *
   * @param duration the duration
   * @param timeUnit the time unit of the duration
   */
  public void jumpToFuture(long duration, TimeUnit timeUnit) {
    long durationInMillis = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
    do {
      durationInMillis = mDeltaQueue.tick(durationInMillis);
      runUntilIdle();
    } while (!mDeltaQueue.isEmpty() && durationInMillis > 0);
  }

  /**
   * Runs all commands/tasks scheduled to be executed immediately but does
   * not jump to future time.
   */
  public void runUntilIdle() {
    while (!schedulerIsIdle()) {
      runNextPendingCommand();
    }
  }

  /**
   * Runs the next mCommand scheduled to be executed immediately.
   */
  public void runNextPendingCommand() {
    ScheduledTask<?> scheduledTask = mDeltaQueue.pop();
    scheduledTask.run();

    if (!scheduledTask.isCancelled() && scheduledTask.repeats()) {
      mDeltaQueue.add(scheduledTask.mRepeatDelay, scheduledTask);
    }
  }

  /**
   * Reports whether scheduler has no commands/tasks pending immediate execution.
   *
   * @return true if there are no commands pending immediate execution, false otherwise
   */
  public boolean schedulerIsIdle() {
    return mDeltaQueue.isEmpty() || mDeltaQueue.delay() > 0;
  }

  @Override
  public void execute(Runnable command) {
    schedule(command, 0, TimeUnit.SECONDS);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    ScheduledTask<Void> task = new ScheduledTask<Void>(command);
    mDeltaQueue.add(TimeUnit.MILLISECONDS.convert(delay, unit), task);
    return task;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    ScheduledTask<V> task = new ScheduledTask<V>(callable);
    mDeltaQueue.add(TimeUnit.MILLISECONDS.convert(delay, unit), task);
    return task;
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
      long initialDelay, long period, TimeUnit unit) {
    return scheduleWithFixedDelay(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
      long initialDelay, long delay, TimeUnit unit) {
    ScheduledTask<Object> task = new ScheduledTask<>(TimeUnit.MILLISECONDS.convert(delay, unit),
        command);
    mDeltaQueue.add(TimeUnit.MILLISECONDS.convert(initialDelay, unit), task);
    return task;
  }

  @Override
  public <T> Future<T> submit(Callable<T> callable) {
    return schedule(callable, 0, TimeUnit.SECONDS);
  }

  @Override
  public Future<?> submit(Runnable command) {
    return submit(command, null);
  }

  @Override
  public <T> Future<T> submit(Runnable command, T result) {
    return submit(new RunnableToCallableAdapter<T>(command, result));
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                       long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public boolean isShutdown() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public boolean isTerminated() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public List<Runnable> shutdownNow() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * Converts Runnable to Callable.
   */
  private final class RunnableToCallableAdapter<T> implements Callable<T> {
    private final Runnable mRunnable;
    private final T mResult;

    /**
     * Constructs a new {@link RunnableToCallableAdapter}.
     */
    RunnableToCallableAdapter(Runnable runnable, T result) {
      mRunnable = runnable;
      mResult = result;
    }

    @Override
    public String toString() {
      return mRunnable.toString();
    }

    @Override
    public T call() throws Exception {
      mRunnable.run();
      return mResult;
    }
  }

  /**
   * Scheduled tasks support repeated tasks.
   */
  private final class ScheduledTask<T> implements ScheduledFuture<T>, Runnable {
    private final long mRepeatDelay;
    private final Callable<T> mCommand;

    private T mFutureResult;
    private Exception mFailure = null;
    private boolean mIsCancelled = false;
    private boolean mIsDone = false;

    /**
     * Constructs a new {@link ScheduledTask} with callable mCommand.
     *
     * @param command a callable mCommand
     */
    public ScheduledTask(Callable<T> command) {
      mRepeatDelay = -1;
      mCommand = command;
    }

    /**
     * Constructs a new {@link ScheduledTask} with mRunnable mCommand.
     *
     * @param command a mRunnable mCommand
     */
    public ScheduledTask(Runnable command) {
      this(-1, command);
    }

    /**
     * Constructs a new {@link ScheduledTask} with repeat delay.
     *
     * @param repeatDelay the delay for repeated tasks
     * @param command a mRunnable mCommand
     */
    public ScheduledTask(long repeatDelay, Runnable command) {
      mRepeatDelay = repeatDelay;
      mCommand = new RunnableToCallableAdapter<T>(command, null);
    }

    /**
     * @return whether or not this task is a repeated task
     */
    public boolean repeats() {
      return mRepeatDelay >= 0;
    }

    @Override
    public String toString() {
      return mCommand.toString() + " mRepeatDelay=" + mRepeatDelay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(mDeltaQueue.delay(this), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      mIsCancelled = true;
      return mDeltaQueue.remove(this);
    }

    @Override
    public boolean isCancelled() {
      return mIsCancelled;
    }

    @Override
    public boolean isDone() {
      return mIsDone;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      if (!mIsDone) {
        throw new UnsupportedOperationException("Operation not supported");
      }
      if (mFailure != null) {
        throw new ExecutionException(mFailure);
      }
      return mFutureResult;
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return get();
    }

    @Override
    public void run() {
      try {
        mFutureResult = mCommand.call();
      } catch (Exception e) {
        mFailure = e;
      }
      mIsDone = true;
    }

    @Override
    public int compareTo(Delayed o) {
      throw new UnsupportedOperationException("Operation not supported");
    }
  }
}
