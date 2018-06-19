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

package alluxio.master;

import alluxio.Constants;
import alluxio.util.ThreadUtils;

/**
 * Test utilities.
 */
public class TestUtils {
  private static final int SERVER_START_TIMEOUT_MS = Constants.MINUTE_MS;

  /**
   * @param process the process to wait for
   */
  public static void waitForReady(alluxio.Process process) {
    if (!process.waitForReady(SERVER_START_TIMEOUT_MS)) {
      ThreadUtils.logAllThreads();
      throw new RuntimeException(
          String.format("Timed out waiting for process %s to start", process));
    }
  }
}
