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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility method for working with threads.
 */
public final class ThreadUtils {
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

  private ThreadUtils() {} // prevent instantiation of utils class
}
