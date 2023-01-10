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

import alluxio.annotation.SuppressFBWarnings;

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
 * A controllable scheduler supports jump to a future time and run all
 * commands within that time period.
 * This class is designed for tests using {@link ScheduledExecutorService}.
 */
public class ControllableScheduler implements ScheduledExecutorService {
  private final ControllableQueue<ScheduledTask<?>> mQueue = new ControllableQueue<>();

  /**
   * Jumps to a future time by a given duration. All the commands/tasks scheduled
   * for execution during this period will be executed. When this function returns,
   * the executor will be idle.
   *
   * @param duration the duration
   * @param timeUnit the time unit of the duration
   */
  public void jumpAndExecute(long duration, TimeUnit timeUnit) {
    long durationInMillis = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
    mQueue.tick(durationInMillis);
    while (!schedulerIsIdle()) {
      runNextPendingCommand();
    }
  }

  /**
   * Reports whether scheduler has no commands/tasks pending immediate execution.
   *
   * @return true if there are no commands pending immediate execution, false otherwise
   */
  public boolean schedulerIsIdle() {
    return mQueue.isEmpty() || mQueue.getHeadDelay() > 0;
  }

  /**
   * Runs the next command scheduled to be executed immediately.
   */
  public void runNextPendingCommand() {
    long peakDelay = mQueue.getHeadDelay();
    ScheduledTask<?> scheduledTask = mQueue.pop();
    scheduledTask.run();
    if (!scheduledTask.isCancelled() && scheduledTask.isRepeat()) {
      mQueue.add(scheduledTask.mRepeatDelay + peakDelay, scheduledTask);
    }
  }

  @Override
  public void execute(Runnable command) {
    schedule(command, 0, TimeUnit.SECONDS);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    ScheduledTask<Void> task = new ScheduledTask<Void>(command);
    mQueue.add(TimeUnit.MILLISECONDS.convert(delay, unit), task);
    return task;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    ScheduledTask<V> task = new ScheduledTask<V>(callable);
    mQueue.add(TimeUnit.MILLISECONDS.convert(delay, unit), task);
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
    mQueue.add(TimeUnit.MILLISECONDS.convert(initialDelay, unit), task);
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
   * Converts runnable to callable.
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
   * {@link ScheduledTask} is a {@link ScheduledFuture} with extra supports for
   * repeated tasks.
   */
  @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
  private final class ScheduledTask<T> implements ScheduledFuture<T>, Runnable {
    private final long mRepeatDelay;
    private final Callable<T> mCommand;

    private T mFutureResult;
    private Exception mFailure = null;
    private boolean mIsCancelled = false;
    private boolean mIsDone = false;

    /**
     * Constructs a new {@link ScheduledTask} with a callable command.
     *
     * @param command a callable command
     */
    public ScheduledTask(Callable<T> command) {
      mRepeatDelay = -1;
      mCommand = command;
    }

    /**
     * Constructs a new {@link ScheduledTask} with a runnable command.
     *
     * @param command a runnable command
     */
    public ScheduledTask(Runnable command) {
      this(-1, command);
    }

    /**
     * Constructs a new {@link ScheduledTask} with repeat delay.
     *
     * @param repeatDelay the delay for repeated tasks
     * @param command a runnable command
     */
    public ScheduledTask(long repeatDelay, Runnable command) {
      mRepeatDelay = repeatDelay;
      mCommand = new RunnableToCallableAdapter<T>(command, null);
    }

    /**
     * @return whether or not this task is a repeated task
     */
    public boolean isRepeat() {
      return mRepeatDelay >= 0;
    }

    @Override
    public String toString() {
      return mCommand.toString() + " mRepeatDelay=" + mRepeatDelay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      mIsCancelled = true;
      return mQueue.remove(this);
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
