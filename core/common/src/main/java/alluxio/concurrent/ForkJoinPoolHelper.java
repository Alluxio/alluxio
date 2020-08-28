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

import alluxio.Constants;
import alluxio.concurrent.jsr.ForkJoinPool;
import alluxio.util.logging.SamplingLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;

/**
 * Provides helpers for working with {@link ForkJoinPool}.
 */
public class ForkJoinPoolHelper {
  private static final Logger SAMPLING_LOG = new SamplingLogger(
      LoggerFactory.getLogger(ForkJoinPoolHelper.class), 30L * Constants.SECOND_MS);

  /**
   * Does managed blocking on ForkJoinPool. This helper is guaranteed to block even when
   * ForkJoinPool is running on full capacity.
   *
   * @param blocker managed blocker resource
   *
   * @throws InterruptedException
   */
  public static void safeManagedBlock(ForkJoinPool.ManagedBlocker blocker)
      throws InterruptedException {
    try {
      ForkJoinPool.managedBlock(blocker);
    } catch (RejectedExecutionException re) {
      SAMPLING_LOG.warn("Failed to compensate rpc pool. Consider increasing thread pool size.", re);
      // Fall back to regular block on given blocker.
      do {
      } while (!blocker.isReleasable() && !blocker.block());
    }
  }
}
