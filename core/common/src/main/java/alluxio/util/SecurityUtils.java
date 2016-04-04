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
import alluxio.security.authentication.AuthType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for security.
 */
@ThreadSafe
public final class SecurityUtils {
  private SecurityUtils() {} // prevent instantiation

  /**
   * Checks if security is enabled.
   *
   * @param conf the configuration for Alluxio
   * @return true if security is enabled, false otherwise
   */
  public static boolean isSecurityEnabled(Configuration conf) {
    return isAuthenticationEnabled(conf) && isAuthorizationEnabled(conf);
  }

  /**
   * Checks if authentication is enabled.
   *
   * @param conf the configuration for Alluxio
   * @return true if authentication is enabled, false otherwise
   */
  public static boolean isAuthenticationEnabled(Configuration conf) {
    return !conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL);
  }

  /**
   * Checks if authorization is enabled.
   *
   * @param conf the configuration for Alluxio
   * @return true if authorization is enabled, false otherwise
   */
  public static boolean isAuthorizationEnabled(Configuration conf) {
    return conf.getBoolean(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
  }
}
