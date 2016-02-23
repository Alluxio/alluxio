/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities class for Alluxio Client. All methods and variables are static.
 */
@ThreadSafe
public final class ClientUtils {
  private static Random sRandom = new Random();

  /**
   * @return a random long which is guaranteed to be non negative (zero is allowed)
   */
  public static synchronized long getRandomNonNegativeLong() {
    return Math.abs(sRandom.nextLong());
  }

  // Prevent instantiation
  private ClientUtils() {}
}
