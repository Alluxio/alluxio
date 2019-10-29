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
public class AdvanceFuture {
  /** Internal list of advance threads. */
  private List<AbstractAdvanceThread> mAdvanceThreads;

  /**
   * Creates a tracker for given advance thread.
   *
   * @param advanceThread journal advance thread
   */
  public AdvanceFuture(AbstractAdvanceThread advanceThread) {
    mAdvanceThreads = new ArrayList<>(1);
    mAdvanceThreads.add(advanceThread);
  }

  /**
   * Used internally to manage multiple advance threads.
   *
   * @param advanceThreads list of advance threads
   */
  private AdvanceFuture(List<AbstractAdvanceThread> advanceThreads) {
    mAdvanceThreads = advanceThreads;
  }

  /**
   * Cancels advance progress gracefully.
   */
  public void cancel() {
    for (AbstractAdvanceThread thread : mAdvanceThreads) {
      thread.cancel();
    }
  }

  /**
   * Waits until advancing finishes.
   */
  public void waitTermination() {
    for (AbstractAdvanceThread thread : mAdvanceThreads) {
      thread.waitTermination();
    }
  }

  /**
   * Creates a single future for tracking a list of advance futures.
   *
   * @param futures list of advance futures
   * @return unified advance future
   */
  public static AdvanceFuture allOf(List<AdvanceFuture> futures) {
    List<AbstractAdvanceThread> threads = new ArrayList<>(futures.size());
    for (AdvanceFuture future : futures) {
      threads.addAll(future.mAdvanceThreads);
    }
    return new AdvanceFuture(threads);
  }

  /**
   * @return a completed advance future
   */
  public static AdvanceFuture completed() {
    return new AdvanceFuture(Collections.emptyList());
  }
}
