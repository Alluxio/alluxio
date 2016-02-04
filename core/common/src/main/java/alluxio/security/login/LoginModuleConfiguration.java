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

package alluxio.security.login;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

import alluxio.security.authentication.AuthType;

/**
 * A JAAS configuration that defines the login modules, by which JAAS uses to login.
 *
 * In implementation, we define several modes (Simple, Kerberos, ...) by constructing different
 * arrays of AppConfigurationEntry, and select the proper array based on the configured mode.
 *
 * Then JAAS login framework use the selected array of AppConfigurationEntry to determine the login
 * modules to be used.
 */
@ThreadSafe
public final class LoginModuleConfiguration extends Configuration {

  private static final Map<String, String> EMPTY_JAAS_OPTIONS = new HashMap<String, String>();

  /** Login module that allows a user name provided by OS. */
  private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
      new AppConfigurationEntry(LoginModuleConfigurationUtils.OS_LOGIN_MODULE_NAME,
          LoginModuleControlFlag.REQUIRED, EMPTY_JAAS_OPTIONS);

  /** Login module that allows a user name provided by application to be specified. */
  private static final AppConfigurationEntry APP_LOGIN = new AppConfigurationEntry(
      AppLoginModule.class.getName(), LoginModuleControlFlag.SUFFICIENT, EMPTY_JAAS_OPTIONS);

  /** Login module that allows a user name provided by a Tachyon specific login module. */
  private static final AppConfigurationEntry TACHYON_LOGIN = new AppConfigurationEntry(
      AlluxioLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, EMPTY_JAAS_OPTIONS);

  // TODO(dong): add Kerberos_LOGIN module
  // private static final AppConfigurationEntry KERBEROS_LOGIN = ...

  /**
   * In {@link AuthType#SIMPLE} mode, JAAS first tries to retrieve the user name set by the
   * application with {@link alluxio.security.login.AppLoginModule}. Upon failure, it uses the OS
   * specific login module to fetch the OS user, and then uses the {@link alluxio.security.login
   * .TachyonLoginModule} to convert it to a Tachyon user represented by
   * {@link alluxio.security.User}. In {@link AuthType#CUSTOM} mode, we also use this configuration.
   */
  private static final AppConfigurationEntry[] SIMPLE =
      new AppConfigurationEntry[] {APP_LOGIN, OS_SPECIFIC_LOGIN, TACHYON_LOGIN};

  // TODO(dong): add Kerberos mode
  // private static final AppConfigurationEntry[] KERBEROS = ...

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    if (appName.equalsIgnoreCase(AuthType.SIMPLE.getAuthName())
        || appName.equalsIgnoreCase(AuthType.CUSTOM.getAuthName())) {
      return SIMPLE;
    } else if (appName.equalsIgnoreCase(AuthType.KERBEROS.getAuthName())) {
      // TODO(dong): return KERBEROS;
      throw new UnsupportedOperationException("Kerberos is not supported currently.");
    }
    return null;
  }
}
