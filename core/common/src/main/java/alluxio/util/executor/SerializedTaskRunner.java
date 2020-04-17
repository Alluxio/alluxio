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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class can execute a series of tasks on a separate thread in the order they are submitted.
 * No guarantees can be made about the timing of how the tasks complete, or what the result of
 * the tasks are.
 *
 * All tasks in the queue are guaranteed to complete before {@link #shutdown(long, TimeUnit)} is
 * called so long as they complete within the provided timeout. At which time the task runner will
 * stop accepting new tasks (all calls to {@link #addTask(Runnable)} will return false).
 *
 */
public class SerializedTaskRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SerializedTaskRunner.class);

  private volatile boolean mRunning;
  private volatile boolean mIsShutdown;
  private final LinkedBlockingQueue<Runnable> mTasks;
  private volatile Thread mRunningThread;

  /**
   * Create a new instance of {@link SerializedTaskRunner}.
   */
  public SerializedTaskRunner() {
    mTasks = new LinkedBlockingQueue<>();
    mRunning = true;
    mIsShutdown = false;
  }

  /**
   * Add a new task to execute into the queue.
   *
   * @param r task to run
   *
   * @return true if the task was added to the queue successfully
   */
  public boolean addTask(Runnable r) {
    if (mIsShutdown) {
      return false;
    }
    return mTasks.add(r);
  }

  @Override
  public void run() {
    mRunningThread = Thread.currentThread();
    while (mRunning) {
      try {
        Runnable task = mTasks.take();
        try {
          task.run();
        } catch (RuntimeException e) {
          LOG.warn("Exception in serialized task runner: ", e);
        }
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for task");
      }
    }
  }

  /**
   * Shuts down the task runner.
   *
   * No new tasks are accepted after this method is called. Remaining tasks are attempted to all
   * be executed within the shutdown timeout. Otherwise, after the timeout the running thread
   * will exit, any remaining tasks will fail to ever be executed.
   *
   * @param time the time to spend before aborting any outstanding tasks
   * @param unit the {@link TimeUnit} corresponding to the time argument
   */
  public synchronized void shutdown(long time, TimeUnit unit) {
    Preconditions.checkNotNull(unit, "time unit");
    if (mIsShutdown) {
      return;
    }
    mIsShutdown = true;
    long totalTime = unit.toMillis(time);
    long curr;
    long start = curr = System.currentTimeMillis();
    while (mTasks.size() > 0 || curr - start > totalTime) {
      curr = System.currentTimeMillis();
      Thread.yield();
    }
    mRunning = false;
    mRunningThread.interrupt();
    mRunningThread = null;
  }

  /**
   * A Factory which can spawn new {@link SerializedTaskRunner}s on separate threads using an
   * available {@link ExecutorService}.
   */
  public static class Factory {
    private final ExecutorService mService;

    /**
     * Create a new instance of  {@link Factory}.
     *
     * @param svc the executor service backing this factory
     */
    public Factory(ExecutorService svc) {
      mService = svc;
    }

    /**
     * Spawn a new task executor which can begin queueing new tasks.
     *
     * It is not guaranteed to immediately begin running tasks. It will depend on the underlying
     * executor's ability to allocate a thread to this task runner.
     *
     * @return a new {@link SerializedTaskRunner}
     */
    public SerializedTaskRunner create() {
      SerializedTaskRunner runner = new SerializedTaskRunner();
      mService.execute(runner);
      return runner;
    }
  }
}
