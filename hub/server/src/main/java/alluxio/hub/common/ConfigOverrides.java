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

package alluxio.hub.common;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;

/**
 * Override some Alluxio configuration values with hub-specific ones.
 */
public class ConfigOverrides {

  /**
   * Overrides the necessary Alluxio configuration with the necessary values for the hub manager
   * and agent.
   *
   * @param conf the conf to override
   */
  public static void overrideConfigs(InstancedConfiguration conf) {
    conf.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.name());
  }

  private ConfigOverrides() {
  }
}
