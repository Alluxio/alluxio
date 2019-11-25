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
  private List<AbstractCatchupThread> mCatchupThreads;

  /**
   * Creates a single future for tracking a list of catchup futures.
   *
   * @param futures list of catchup futures
   * @return unified catchup future
   */
  public static CatchupFuture allOf(List<CatchupFuture> futures) {
    List<AbstractCatchupThread> threads = new ArrayList<>(futures.size());
    for (CatchupFuture future : futures) {
      threads.addAll(future.mCatchupThreads);
    }
    return new CatchupFuture(threads);
  }

  /**
   * @return a completed catchup future
   */
  public static CatchupFuture completed() {
    return new CatchupFuture(Collections.emptyList());
  }

  /**
   * Creates a tracker for given catchup thread.
   *
   * @param catchupThread journal catchup thread
   */
  public CatchupFuture(AbstractCatchupThread catchupThread) {
    mCatchupThreads = new ArrayList<>(1);
    mCatchupThreads.add(catchupThread);
  }

  /**
   * Used internally to manage multiple catchup threads.
   *
   * @param catchupThreads list of catchup threads
   */
  private CatchupFuture(List<AbstractCatchupThread> catchupThreads) {
    mCatchupThreads = catchupThreads;
  }

  /**
   * Cancels catchup progress gracefully.
   */
  public void cancel() {
    for (AbstractCatchupThread thread : mCatchupThreads) {
      thread.cancel();
    }
  }

  /**
   * Waits until advancing finishes.
   */
  public void waitTermination() {
    for (AbstractCatchupThread thread : mCatchupThreads) {
      thread.waitTermination();
    }
  }
}
