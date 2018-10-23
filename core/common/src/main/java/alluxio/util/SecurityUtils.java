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

package alluxio.util;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.LoginUser;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;

import java.io.IOException;

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
   * @return true if security is enabled, false otherwise
   */
  public static boolean isSecurityEnabled() {
    return isAuthenticationEnabled() && isAuthorizationEnabled();
  }

  /**
   * Checks if authentication is enabled.
   *
   * @return true if authentication is enabled, false otherwise
   */
  public static boolean isAuthenticationEnabled() {
    return !Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL);
  }

  /**
   * Checks if authorization is enabled.
   *
   * @return true if authorization is enabled, false otherwise
   */
  public static boolean isAuthorizationEnabled() {
    return Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
  }

  /**
   * @return the owner fetched from the login module, or empty string if the fetch fails or
   *         authentication is disabled
   */
  public static String getOwnerFromLoginModule() {
    try {
      return LoginUser.get().getName();
    } catch (UnauthenticatedException | UnsupportedOperationException e) {
      return "";
    }
  }

  /**
   * @return the owner fetched from the Thrift client, or empty string if the fetch fails or
   *         authentication is disabled
   */
  public static String getOwnerFromThriftClient() {
    try {
      User user = AuthenticatedClientUser.get();
      if (user == null) {
        return "";
      }
      return user.getName();
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * @return the group fetched from the login module, or empty string if the fetch fails or
   *         authentication is disabled
   */
  public static String getGroupFromLoginModule() {
    try {
      return CommonUtils.getPrimaryGroupName(LoginUser.get().getName());
    } catch (IOException | UnsupportedOperationException e) {
      return "";
    }
  }

  /**
   * @return the group fetched from the Thrift client, or empty string if the fetch fails or
   *         authentication is disabled
   */
  public static String getGroupFromThriftClient() {
    try {
      User user = AuthenticatedClientUser.get();
      if (user == null) {
        return "";
      }
      return CommonUtils.getPrimaryGroupName(user.getName());
    } catch (IOException e) {
      return "";
    }
  }
}
