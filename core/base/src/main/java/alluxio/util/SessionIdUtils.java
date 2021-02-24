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

import java.security.SecureRandom;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for working with a session id in Alluxio.
 */
@ThreadSafe
public final class SessionIdUtils {
  private static SecureRandom sRandom = new SecureRandom();

  private SessionIdUtils() {} // prevent instantiation

  /**
   * Generates a positive random number by zero-ing the sign bit.
   *
   * @return a random long which is guaranteed to be non negative (zero is allowed)
   */
  public static synchronized long getRandomNonNegativeLong() {
    return sRandom.nextLong() & Long.MAX_VALUE;
  }

  /**
   * @return a session ID
   */
  public static long createSessionId() {
    return getRandomNonNegativeLong();
  }
}
