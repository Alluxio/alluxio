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

package alluxio.table.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utility methods for configuration.
 */
public class ConfigurationUtils {
  public static final String MOUNT_PREFIX = "mount.option.";
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

  private ConfigurationUtils() {} // prevent instantiation

  /**
   * @param udbType the udb type
   * @return the prefix of the property name, for a given udb type
   */
  public static String getUdbPrefix(String udbType) {
    return String.format("udb-%s.", udbType);
  }
}
