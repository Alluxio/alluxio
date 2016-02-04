/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

/**
 * A set of utility methods for testing concurrency.
 */
public final class ConcurrencyTestUtils {
  private ConcurrencyTestUtils() {} // prevent instantiation

  /**
   * Tests the current operations of a list of runnables. Run all the operations at the same time to
   * maximize the chances of triggering a multithreading code error. Suggested by junit team at
   * https://github.com/junit-team/junit/wiki/Multithreaded-code-and-concurrency.
   *
   * @param runnables
   * @param maxTimeoutSeconds
   * @throws InterruptedException
   */
  public static void assertConcurrent(final List<? extends Runnable> runnables,
      final int maxTimeoutSeconds) throws InterruptedException {
    final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    try {
      final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
      final CountDownLatch afterInitBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      for (final Runnable submittedTestRunnable : runnables) {
        threadPool.submit(new Runnable() {
          @Override
          public void run() {
            allExecutorThreadsReady.countDown();
            try {
              afterInitBlocker.await();
              submittedTestRunnable.run();
            } catch (final Throwable e) {
              exceptions.add(e);
            } finally {
              allDone.countDown();
            }
          }
        });
      }
      // wait until all threads are ready
      Assert.assertTrue("Timeout initializing threads!",
          allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));

      // start all test runners
      afterInitBlocker.countDown();
      Assert.assertTrue("Timeout! More than " + maxTimeoutSeconds + " seconds",
          allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
    } finally {
      threadPool.shutdownNow();
    }
    Assert.assertTrue("Failed with exception(s) " + exceptions, exceptions.isEmpty());
  }
}
