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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.authentication.AuthenticationFactory;
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

  /** The configuration to use */
  private static TachyonConf sConf;
  /** The authentication method to use */
  private static AuthenticationFactory.AuthTypes sAuthType;
  /** UserInformation about the login user */
  private static UserInformation sLoginUser;

  /**
   * The subject of this UserInformation instance. A subject contains several perspective of a
   * user.
   */
  private final Subject mSubject;
  /** The Tachyon user represented by this UserInformation instance. */
  private final User mUser;

  static {
    OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
    OS_PRINCIPAL_CLASS_NAME = findOsPrincipalClassName();
  }

  /**
   * Create a UserInformation for the given subject.
   * @param subject the user's subject
   */
  UserInformation(Subject subject) {
    mSubject = subject;
    mUser = subject.getPrincipals(User.class).iterator().next();
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
      if (appName.equalsIgnoreCase(AuthenticationFactory.AuthTypes.SIMPLE.getAuthName())) {
        return SIMPLE;
      } else if (appName.equalsIgnoreCase(AuthenticationFactory.AuthTypes.KERBEROS.getAuthName())) {
        // TODO: return KERBEROS;
        throw new UnsupportedOperationException("Kerberos is not supported currently.");
      }
      return null;
    }
  }

  /**
   * Set the static configuration and initialize based on it
   * @param conf the configuration
   */
  public static void setTachyonConf(TachyonConf conf) {
    initialize(conf);
  }

  /**
   * Check whether fields are initialized based on the configuration.
   */
  private static void ensureInitialized() {
    if (sConf == null) {
      initialize(new TachyonConf());
    }
  }

  /**
   * Initialize with conf
   * @param conf the configuration
   */
  private static void initialize(TachyonConf conf) {
    sAuthType = AuthenticationFactory.getAuthTypeFromConf(conf);
    if (sAuthType == AuthenticationFactory.AuthTypes.NOSASL) {
      throw new UnsupportedOperationException("UserInformation is not supported in NOSASL mode");
    }

    sConf = conf;

    // TODO: other init work.
  }

  /**
   * Get current login user
   * @return the login user
   * @throws IOException if login fails
   */
  public static UserInformation getTachyonLoginUser() throws IOException {
    if (sLoginUser == null) {
      login();
    }
    return sLoginUser;
  }

  /**
   * Login based on the LoginModules
   * @throws IOException if login fails
   */
  public static void login() throws IOException {
    ensureInitialized();
    try {
      Subject subject = new Subject();

      LoginContext loginContext = new LoginContext(sAuthType.getAuthName(), subject, null,
          new TachyonJaasConfiguration());
      loginContext.login();

      sLoginUser = new UserInformation(subject);
    } catch (LoginException e) {
      throw new IOException("fail to login", e);
    }
  }

  public String getUserName() {
    return mUser.getName();
  }

  @VisibleForTesting
  static void reset() {
    sConf = null;
    sLoginUser = null;
    sAuthType = null;
  }

  // TODO: createRemoteUser(), used for remote user in RPC
}
