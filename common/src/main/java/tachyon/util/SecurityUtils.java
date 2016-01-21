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

package tachyon.util;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.authentication.AuthType;

/**
 * Utility methods for security.
 */
public final class SecurityUtils {
  private SecurityUtils() {} // prevent instantiation

  /**
   * Checks if security is enabled.
   *
   * @param conf the configuration for Tachyon
   * @return true if security is enabled, false otherwise
   */
  public static boolean isSecurityEnabled(TachyonConf conf) {
    return isAuthenticationEnabled(conf) && isAuthorizationEnabled(conf);
  }

  /**
   * Checks if authentication is enabled.
   *
   * @param conf the configuration for Tachyon
   * @return true if authentication is enabled, false otherwise
   */
  public static boolean isAuthenticationEnabled(TachyonConf conf) {
    return !conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL);
  }

  /**
   * Checks if authorization is enabled.
   *
   * @param conf the configuration for Tachyon
   * @return true if authorization is enabled, false otherwise
   */
  public static boolean isAuthorizationEnabled(TachyonConf conf) {
    return conf.getBoolean(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
  }
}
