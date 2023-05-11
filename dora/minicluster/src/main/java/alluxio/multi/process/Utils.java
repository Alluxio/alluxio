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

package alluxio.multi.process;

import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for external test cluster classes.
 */
@ThreadSafe
public final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Launches a thread to terminate the current process after the specified period has elapsed.
   *
   * @param lifetimeMs the time to wait before terminating the current process
   */
  public static void limitLife(final long lifetimeMs) {
    new Thread(() -> {
      CommonUtils.sleepMs(lifetimeMs);
      LOG.info("Process has timed out after {}ms, exiting now", lifetimeMs);
      System.exit(-1);
    }, "life-limiter").start();
  }

  private Utils() {} // Not intended for instantiation.
}
