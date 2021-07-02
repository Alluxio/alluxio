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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.UserState;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Utility methods for security.
 */
@ThreadSafe
public final class SecurityUtils {
  private SecurityUtils() {} // prevent instantiation

  /**
   * Checks if security is enabled.
   *
   * @param conf Alluxio configuration
   * @return true if security is enabled, false otherwise
   */
  public static boolean isSecurityEnabled(AlluxioConfiguration conf) {
    return isAuthenticationEnabled(conf) && isAuthorizationEnabled(conf);
  }

  /**
   * Checks if authentication is enabled.
   *
   * @param conf Alluxio configuration
   * @return true if authentication is enabled, false otherwise
   */
  public static boolean isAuthenticationEnabled(AlluxioConfiguration conf) {
    return !conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL);
  }

  /**
   * Checks if authorization is enabled.
   *
   * @param conf Alluxio configuration
   * @return true if authorization is enabled, false otherwise
   */
  public static boolean isAuthorizationEnabled(AlluxioConfiguration conf) {
    return conf.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
  }

  /**
   * @param conf Alluxio configuration
   * @return the owner fetched from the gRPC client, or empty string if the fetch fails or
   *         authentication is disabled
   */
  public static String getOwnerFromGrpcClient(AlluxioConfiguration conf) {
    try {
      User user = AuthenticatedClientUser.get(conf);
      if (user == null) {
        return "";
      }
      return user.getName();
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * @param conf Alluxio configuration
   * @return the group fetched from the gRPC client, or empty string if the fetch fails or
   *         authentication is disabled
   */
  public static String getGroupFromGrpcClient(AlluxioConfiguration conf) {
    try {
      User user = AuthenticatedClientUser.get(conf);
      if (user == null) {
        return "";
      }
      return CommonUtils.getPrimaryGroupName(user.getName(), conf);
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * @param userState the UserState
   * @return the owner name
   */
  public static String getOwner(UserState userState) {
    try {
      return userState.getUser().getName();
    } catch (UnauthenticatedException | UnsupportedOperationException e) {
      return "";
    }
  }

  /**
   * @param userState the UserState
   * @param conf the configuration
   * @return the primary group name for the user
   */
  public static String getGroup(UserState userState, AlluxioConfiguration conf) {
    try {
      return CommonUtils.getPrimaryGroupName(getOwner(userState), conf);
    } catch (IOException | UnsupportedOperationException e) {
      return "";
    }
  }

  /**
   * Creates a new {@link LoginContext} with the correct class loader.
   *
   * @param authType the {@link AuthType} to use
   * @param subject the {@link Subject} to use
   * @param classLoader the {@link ClassLoader} to use
   * @param configuration the {@link javax.security.auth.login.Configuration} to use
   * @param callbackHandler the {@link CallbackHandler}
   * @return the new {@link LoginContext} instance
   * @throws LoginException if LoginContext cannot be created
   */
  public static LoginContext createLoginContext(AuthType authType, Subject subject,
      ClassLoader classLoader, Configuration configuration, CallbackHandler callbackHandler)
      throws LoginException {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      // Create LoginContext based on authType, corresponding LoginModule should be registered
      // under the authType name in LoginModuleConfiguration.
      return new LoginContext(authType.getAuthName(), subject, callbackHandler, configuration);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }
}
