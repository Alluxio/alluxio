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

package alluxio.util;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.Constants;
import alluxio.Configuration;

/**
 * Utility methods for lineage.
 */
@ThreadSafe
public final class LineageUtils {
  private LineageUtils() {} // prevent instantiation

  /**
   * Checks if lineage is enabled.
   *
   * @param conf the configuration for Tachyon
   * @return true if lineage is enabled, false otherwise
   */
  public static boolean isLineageEnabled(Configuration conf) {
    return conf.getBoolean(Constants.USER_LINEAGE_ENABLED);
  }
}
