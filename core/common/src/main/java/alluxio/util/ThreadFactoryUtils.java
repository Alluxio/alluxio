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

package alluxio.util;

import alluxio.concurrent.jsr.ForkJoinPool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for the {@link ThreadFactory} class.
 */
@ThreadSafe
public final class ThreadFactoryUtils {
  private ThreadFactoryUtils() {}

  /**
   * Creates a {@link java.util.concurrent.ThreadFactory} that spawns off threads.
   *
   * @param nameFormat name pattern for each thread. should contain '%d' to distinguish between
   *                   threads.
   * @param isDaemon if true, the {@link java.util.concurrent.ThreadFactory} will create
   *                 daemon threads.
   * @return the created factory
   */
  public static ThreadFactory build(final String nameFormat, boolean isDaemon) {
    return new ThreadFactoryBuilder().setDaemon(isDaemon).setNameFormat(nameFormat).build();
  }

  /**
   * Creates a {@link ForkJoinPool.ForkJoinWorkerThreadFactory} that spawns off threads
   * for {@link ForkJoinPool}.
   *
   * @param nameFormat name pattern for each thread. should contain '%d' to distinguish between
   *                   threads.
   * @param isDaemon if true, the {@link java.util.concurrent.ThreadFactory} will create
   *                 daemon threads.
   * @return the created factory
   */
  public static ForkJoinPool.ForkJoinWorkerThreadFactory buildFjp(final String nameFormat,
      boolean isDaemon) {
    return new ForkJoinPool.AlluxioForkJoinWorkerThreadFactory(nameFormat, isDaemon);
  }
}
