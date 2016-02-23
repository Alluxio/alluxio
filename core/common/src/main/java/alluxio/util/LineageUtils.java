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

package alluxio.util;

import alluxio.Configuration;
import alluxio.Constants;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for lineage.
 */
@ThreadSafe
public final class LineageUtils {
  private LineageUtils() {} // prevent instantiation

  /**
   * Checks if lineage is enabled.
   *
   * @param conf the configuration for Alluxio
   * @return true if lineage is enabled, false otherwise
   */
  public static boolean isLineageEnabled(Configuration conf) {
    return conf.getBoolean(Constants.USER_LINEAGE_ENABLED);
  }
}
