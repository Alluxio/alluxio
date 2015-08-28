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

import tachyon.util.PlatformUtils;

/**
 * This class collects constants used in JAAS login.
 */
public class TachyonJaasProperties {

  private static final String OS_LOGIN_MODULE_NAME;
  private static final String OS_PRINCIPAL_CLASS_NAME;
  private static final boolean WINDOWS = PlatformUtils.OS_NAME.startsWith("Windows");
  private static final boolean IS_64_BIT = PlatformUtils.PROCESSOR_BIT.contains("64");
  private static final boolean AIX = PlatformUtils.OS_NAME.equals("AIX");

  static {
    OS_LOGIN_MODULE_NAME = findOSLoginModuleName();
    OS_PRINCIPAL_CLASS_NAME = findOsPrincipalClassName();
  }

  // Return the OS login module class name.
  private static String findOSLoginModuleName() {
    if (PlatformUtils.IBM_JAVA) {
      if (WINDOWS) {
        return IS_64_BIT ? "com.ibm.security.auth.module.Win64LoginModule"
            : "com.ibm.security.auth.module.NTLoginModule";
      } else if (AIX) {
        return IS_64_BIT ? "com.ibm.security.auth.module.AIX64LoginModule"
            : "com.ibm.security.auth.module.AIXLoginModule";
      } else {
        return "com.ibm.security.auth.module.LinuxLoginModule";
      }
    } else {
      return WINDOWS ? "com.sun.security.auth.module.NTLoginModule"
          : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  // Return the OS principal class name
  private static String findOsPrincipalClassName() {
    String principalClassName = null;
    if (PlatformUtils.IBM_JAVA) {
      if (IS_64_BIT) {
        principalClassName = "com.ibm.security.auth.UsernamePrincipal";
      } else {
        if (WINDOWS) {
          principalClassName = "com.ibm.security.auth.NTUserPrincipal";
        } else if (AIX) {
          principalClassName = "com.ibm.security.auth.AIXPrincipal";
        } else {
          principalClassName = "com.ibm.security.auth.LinuxPrincipal";
        }
      }
    } else {
      principalClassName = WINDOWS ? "com.sun.security.auth.NTUserPrincipal"
          : "com.sun.security.auth.UnixPrincipal";
    }
    return principalClassName;
  }

  static String getOsLoginModuleName() {
    return OS_LOGIN_MODULE_NAME;
  }

  static String getOsPrincipalClassName() {
    return OS_PRINCIPAL_CLASS_NAME;
  }
}
