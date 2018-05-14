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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility method for working with threads.
 */
public final class ThreadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadUtils.class);

  /**
   * @param thread a thread
   * @return a human-readable representation of the thread's stack trace
   */
  public static String formatStackTrace(Thread thread) {
    Throwable t = new Throwable(String.format("Stack trace for thread %s:", thread.getName()));
    t.setStackTrace(thread.getStackTrace());
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * From https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
   *
   * The following method shuts down an ExecutorService in two phases, first by calling shutdown to
   * reject incoming tasks, and then calling shutdownNow, if necessary, to cancel any lingering
   * tasks.
   *
   * @param pool the executor service to shutdown
   */
  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
          LOG.warn("Pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  private ThreadUtils() {} // prevent instantiation of utils class
}
