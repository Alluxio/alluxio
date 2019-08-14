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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.io.InterruptedIOException;

/**
 * Utility methods for working with exceptions.
 */
public final class ExceptionUtils {
  /**
   * @param t a throwable to check
   * @return whether the given throwable contains a type of interrupted exception in its causal
   *         chain (the causal chain includes the throwable itself)
   */
  public static boolean containsInterruptedException(Throwable t) {
    return !Iterables
        .isEmpty(Iterables.filter(Throwables.getCausalChain(t), x -> isInterrupted(x)));
  }

  /**
   * @param t a throwable to check
   * @return whether the throwable is a type of interrupted exception
   */
  public static boolean isInterrupted(Throwable t) {
    return t instanceof InterruptedException || t instanceof InterruptedIOException;
  }

  private ExceptionUtils() {} // Utils class
}
