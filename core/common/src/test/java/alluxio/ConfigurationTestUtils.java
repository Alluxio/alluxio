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

package alluxio;

/**
 * Utility methods for the configuration tests.
 */
public final class ConfigurationTestUtils {

  /**
   * Resets the configuration to its initial state.
   *
   * This method should only be used as a cleanup mechanism between tests. It should not be used
   * while any object may be using the {@link Configuration}.
   */
  public static void resetConfiguration() {
    Configuration.init();
  }

  private ConfigurationTestUtils() {} // prevent instantiation
}
