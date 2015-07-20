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

package tachyon.security;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.util.PlatformUtils;

/**
 * This class wraps a JAAS Subject and provides methods to determine the user. It supports
 * Windows, Unix, and Kerberos login modules.
 */
public class UserInformation {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final String OS_LOGIN_MODULE_NAME;
  private static final String OS_PRINCIPAL_CLASS_NAME;
  private static final boolean WINDOWS = PlatformUtils.OS_NAME.startsWith("Windows");
  private static final boolean IS_64_BIT = PlatformUtils.PROCESSOR_BIT.contains("64");
  private static final boolean AIX = PlatformUtils.OS_NAME.equals("AIX");

  static {
    OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
    OS_PRINCIPAL_CLASS_NAME = findOsPrincipalClassName();
  }

  // Return the OS login module class name.
  private static String getOSLoginModuleName() {
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

  static String getOsPrincipalClassName() {
    return OS_PRINCIPAL_CLASS_NAME;
  }

  /**
   * A JAAS configuration that defines the login modules, by which we use to login.
   */
  static class TachyonJaasConfiguration extends Configuration {
    private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<String,String>();

    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
        new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);

    private static final AppConfigurationEntry TACHYON_LOGIN =
        new AppConfigurationEntry(TachyonLoginModule.class.getName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);

    // TODO: add Kerberos_LOGIN module
    // private static final AppConfigurationEntry KERBEROS_LOGIN = ...

    private static final AppConfigurationEntry[] SIMPLE = new
        AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, TACHYON_LOGIN};

    // TODO: add Kerberos mode
    // private static final AppConfigurationEntry[] KERBEROS = ...

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      // TODO: switch branch based on appName. (Simple, Kerberos, etc)
      return SIMPLE;
    }
  }

  // TODO: TACHYON-613 - add methods to get login user
}
