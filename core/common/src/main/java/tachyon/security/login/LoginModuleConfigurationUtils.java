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

package tachyon.security.login;

import javax.annotation.concurrent.ThreadSafe;

import tachyon.util.OSUtils;

/**
 * This class provides constants used in JAAS login.
 */
@ThreadSafe
public final class LoginModuleConfigurationUtils {
  /** Login module according to different OS type */
  public static final String OS_LOGIN_MODULE_NAME;
  /** Class name of Principal according to different OS type */
  public static final String OS_PRINCIPAL_CLASS_NAME;

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
