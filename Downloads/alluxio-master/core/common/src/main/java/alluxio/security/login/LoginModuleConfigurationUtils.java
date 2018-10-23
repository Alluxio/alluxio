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

package alluxio.security.login;

import alluxio.util.OSUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides constants used in JAAS login.
 */
@ThreadSafe
public final class LoginModuleConfigurationUtils {
  /** Login module according to different OS type. */
  public static final String OS_LOGIN_MODULE_NAME;
  /** Class name of Principal according to different OS type. */
  public static final String OS_PRINCIPAL_CLASS_NAME;

  private LoginModuleConfigurationUtils() {} // prevent instantiation

  static {
    OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
    OS_PRINCIPAL_CLASS_NAME = getOSPrincipalClassName();
  }

  /**
   * @return the OS login module class name
   */
  private static String getOSLoginModuleName() {
    if (OSUtils.IBM_JAVA) {
      if (OSUtils.isWindows()) {
        return OSUtils.is64Bit() ? "com.ibm.security.auth.module.Win64LoginModule"
            : "com.ibm.security.auth.module.NTLoginModule";
      } else if (OSUtils.isAIX()) {
        return OSUtils.is64Bit() ? "com.ibm.security.auth.module.AIX64LoginModule"
            : "com.ibm.security.auth.module.AIXLoginModule";
      } else {
        return "com.ibm.security.auth.module.LinuxLoginModule";
      }
    } else {
      return OSUtils.isWindows() ? "com.sun.security.auth.module.NTLoginModule"
          : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  /**
   * @return the OS principal class name
   */
  private static String getOSPrincipalClassName() {
    String principalClassName;
    if (OSUtils.IBM_JAVA) {
      if (OSUtils.is64Bit()) {
        principalClassName = "com.ibm.security.auth.UsernamePrincipal";
      } else {
        if (OSUtils.isWindows()) {
          principalClassName = "com.ibm.security.auth.NTUserPrincipal";
        } else if (OSUtils.isAIX()) {
          principalClassName = "com.ibm.security.auth.AIXPrincipal";
        } else {
          principalClassName = "com.ibm.security.auth.LinuxPrincipal";
        }
      }
    } else {
      principalClassName = OSUtils.isWindows() ? "com.sun.security.auth.NTUserPrincipal"
          : "com.sun.security.auth.UnixPrincipal";
    }
    return principalClassName;
  }
}
