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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
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
    return new AlluxioForkJoinPoolThreadFactory(nameFormat, isDaemon);
  }

  static class AlluxioForkJoinPoolThreadFactory
      implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    // ForkJoinWorkerThread index counter.
    private static final AtomicLong THREAD_INDEX = new AtomicLong(0);
    // Thread properties.
    private final String mNameFormat;
    private final boolean mIsDaemon;

    public AlluxioForkJoinPoolThreadFactory(String nameFormat, boolean isDaemon) {
      mNameFormat = nameFormat;
      mIsDaemon = isDaemon;
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      return new AlluxioForkJoinPoolThread(pool,
          String.format(mNameFormat, THREAD_INDEX.getAndIncrement()), mIsDaemon);
    }
  }

  static class AlluxioForkJoinPoolThread extends ForkJoinWorkerThread {
    protected AlluxioForkJoinPoolThread(ForkJoinPool pool, String name, boolean isDaemon) {
      super(pool);
      setName(name);
      setDaemon(isDaemon);
    }
  }
}
