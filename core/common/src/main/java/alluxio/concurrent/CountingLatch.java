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

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Similar to {@link java.util.concurrent.CountDownLatch} except that the count can either be
 * increased or decreased.
 *
 * A thread can wait until the count reaches 0.
 * Other threads can increase or decrease the count concurrently.
 * When count reaches 0, if there is a thread waiting for state 0, the thread will be waken up,
 * and other threads trying to increase the count are blocked. If there is no thread waiting for
 * state 0, then other threads can keep increasing the count.
 *
 * Methods can be run concurrently from different threads, the paired methods({@link #inc()} and
 * {@link #dec()}, {@link #await()} and {@link #release()}) do not need to be
 * called in the same thread.
 *
 * An example usage is to synchronize FileSystemContext reinitialization with the ongoing RPCs.
 * See alluxio.client.fs.FileSystemContextReinitializer for more details.
 */
public class CountingLatch {
  /** Represents the ignored arguments for AQS methods. */
  private static final int IGNORED_ARG = -1;
  /** Represents the state where {@link #inc()} is blocked. */
  private static final int BLOCK_INC_STATE = -1;

  /**
   * Meaning of the AQS state:
   * 1. when equals BLOCK_INC_STATE, means awaitAndBlockInc has returned, and all further calls to
   * inc are blocked until unblockInc is called.
   * 2. when >= 0, means the counter value increased by inc and decreased by dec.
   */
  private static final class Sync extends AbstractQueuedSynchronizer {
    private static final long serialVersionUID = 8553010505281724322L;

    @Override
    protected boolean tryAcquire(int arg) {
      return compareAndSetState(0, BLOCK_INC_STATE);
    }

    @Override
    protected boolean tryReleaseShared(int arg) {
      if (!compareAndSetState(BLOCK_INC_STATE, 0)) {
        throw new Error("state must be " + BLOCK_INC_STATE);
      }
      return true;
    }

    @Override
    protected int tryAcquireShared(int arg) {
      while (true) {
        int current = getState();
        if (current == BLOCK_INC_STATE) {
          return -1;
        }
        if (compareAndSetState(current, current + 1)) {
          return 1;
        }
      }
    }

    @Override
    protected boolean tryRelease(int arg) {
      while (true) {
        int current = getState();
        if (current <= 0) {
          throw new Error("state must be > 0");
        }
        int newState = current - 1;
        if (compareAndSetState(current, newState)) {
          // Signal only when transitioning to 0.
          return newState == 0;
        }
      }
    }

    @VisibleForTesting
    int state() {
      return getState();
    }
  }

  private Sync mSync = new Sync();

  /**
   * Increases the counter.
   *
   * If {@link #await()} returns, this call is blocked until {@link #release()} is
   * called.
   *
   * @throws InterruptedException when interrupted during being blocked
   */
  public void inc() throws InterruptedException {
    mSync.acquireSharedInterruptibly(IGNORED_ARG);
  }

  /**
   * Decreases the counter.
   *
   * Should only be called in pair with {@link #inc()}.
   * If counter will go below zero after this call, throws {@link Error}.
   * This method is never blocked.
   */
  public void dec() {
    mSync.release(IGNORED_ARG);
  }

  /**
   * Blocked during awaiting the counter to become zero.
   * When it returns, all further calls to {@link #inc()} are blocked.
   */
  public void await() {
    mSync.acquire(IGNORED_ARG);
  }

  /**
   * Unblocks threads blocked on calling {@link #inc()}.
   *
   * Should only be called in pair with {@link #await()}.
   * If not paired, throws {@link Error}.
   * This method is never blocked.
   */
  public void release() {
    mSync.releaseShared(IGNORED_ARG);
  }

  /**
   * @return the internal AQS state
   */
  @VisibleForTesting
  int getState() {
    return mSync.state();
  }
}
