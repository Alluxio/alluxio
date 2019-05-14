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

package alluxio.test.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A set of utility methods for testing concurrency.
 */
public final class ConcurrencyUtils {
  private ConcurrencyUtils() {} // prevent instantiation

  /**
   * Tests the current operations of a list of runnables. Run all the operations at the same time to
   * maximize the chances of triggering a multithreading code error. Suggested by junit team at
   * https://github.com/junit-team/junit/wiki/Multithreaded-code-and-concurrency.
   *
   * @param runnables the list of runnables
   * @param maxTimeoutSeconds the maximal timeout in seconds
   * @throws InterruptedException if awaiting one of the threads fails
   */
  public static void assertConcurrent(final List<? extends Runnable> runnables,
      final int maxTimeoutSeconds) throws InterruptedException {
    final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    try {
      final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
      final CountDownLatch afterInitBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      for (final Runnable submittedTestRunnable : runnables) {
        threadPool.submit(() -> {
          allExecutorThreadsReady.countDown();
          try {
            afterInitBlocker.await();
            submittedTestRunnable.run();
          } catch (final Throwable e) {
            exceptions.add(e);
          } finally {
            allDone.countDown();
          }
        });
      }
      // wait until all threads are ready
      assertTrue("Timeout initializing threads!",
          allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));

      // start all test runners
      afterInitBlocker.countDown();
      assertTrue("Timeout! More than " + maxTimeoutSeconds + " seconds",
          allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
    } finally {
      threadPool.shutdownNow();
    }
    assertTrue("Failed with exception(s) " + exceptions, exceptions.isEmpty());
  }
}
