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

package alluxio.master.file.cmdmanager.scheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import alluxio.exception.status.ResourceExhaustedException;
import alluxio.master.file.cmdmanager.task.BlockTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class for scheduling distributed commands.
 * @param <T> type of scheduled task
 */
public final class CommandScheduler<T extends BlockTask> {
  private static final Logger LOG = LoggerFactory.getLogger(CommandScheduler.class);
  private static final int CAPACITY = 1000;
  private ExecutorService mExecutorService;
  private final BlockingQueue<T> mCommandQueue = new LinkedBlockingQueue<>(CAPACITY);
  private final long mTimeOut;

  /**
   * Constructor.
   * @param timeout
   */
  public CommandScheduler(long timeout) {
    mExecutorService = Executors.newSingleThreadExecutor();
    mTimeOut = timeout;
  }

  /**
   * Run the scheduler.
   */
  public void start() {
    mExecutorService.submit(() -> {
      try {
        while (!Thread.interrupted()) {
          T task = mCommandQueue.poll();
          task.runBlockTask();
        }
      } catch (InterruptedException e) {
        // Allow thread to exit.
      } catch (Exception e) {
        LOG.error("Uncaught exception in scheduling block tasks", e);
      } finally {
        shutDown();
      }
    });
  }

  /**
   * Schedule to run the task.
   * @param task the task to run
   */
  public void schedule(T task) throws ResourceExhaustedException, InterruptedException {
    boolean offered = mCommandQueue.offer(task, mTimeOut, MILLISECONDS);
    if (!offered) {
      throw new ResourceExhaustedException(
          "Could not enqueue in the scheduler after " + mTimeOut + " ms.");
    }
  }

  /**
   * Shut down the scheduler.
   */
  public void shutDown() {
    mCommandQueue.clear();
  }
}
