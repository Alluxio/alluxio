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

package alluxio.master.journal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Used to track and cancel journal advancing.
 */
public class CatchupFuture {
  /** Internal list of catch-up threads. */
  private List<AbstractCatchupThread> mAdvanceThreads;

  /**
   * Creates a tracker for given advance thread.
   *
   * @param advanceThread journal advance thread
   */
  public CatchupFuture(AbstractCatchupThread advanceThread) {
    mAdvanceThreads = new ArrayList<>(1);
    mAdvanceThreads.add(advanceThread);
  }

  /**
   * Used internally to manage multiple advance threads.
   *
   * @param advanceThreads list of advance threads
   */
  private CatchupFuture(List<AbstractCatchupThread> advanceThreads) {
    mAdvanceThreads = advanceThreads;
  }

  /**
   * Cancels advance progress gracefully.
   */
  public void cancel() {
    for (AbstractCatchupThread thread : mAdvanceThreads) {
      thread.cancel();
    }
  }

  /**
   * Waits until advancing finishes.
   */
  public void waitTermination() {
    for (AbstractCatchupThread thread : mAdvanceThreads) {
      thread.waitTermination();
    }
  }

  /**
   * Creates a single future for tracking a list of advance futures.
   *
   * @param futures list of advance futures
   * @return unified advance future
   */
  public static CatchupFuture allOf(List<CatchupFuture> futures) {
    List<AbstractCatchupThread> threads = new ArrayList<>(futures.size());
    for (CatchupFuture future : futures) {
      threads.addAll(future.mAdvanceThreads);
    }
    return new CatchupFuture(threads);
  }

  /**
   * @return a completed advance future
   */
  public static CatchupFuture completed() {
    return new CatchupFuture(Collections.emptyList());
  }
}
