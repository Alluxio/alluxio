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

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import tachyon.security.authentication.AuthType;

/**
 * A JAAS configuration that defines the login modules, by which JAAS uses to login.
 *
 * In implementation, we define several modes (Simple, Kerberos, ...) by constructing different
 * arrays of AppConfigurationEntry, and select the proper array based on the configured mode.
 *
 * Then JAAS login framework use the selected array of AppConfigurationEntry to determine the login
 * modules to be used.
 */
public final class TachyonJaasConfiguration extends Configuration {

  private static final Map<String, String> EMPTY_JAAS_OPTIONS = new HashMap<String, String>();

  private static final AppConfigurationEntry OS_SPECIFIC_LOGIN = new AppConfigurationEntry(
      TachyonJaasProperties.OS_LOGIN_MODULE_NAME,
      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, EMPTY_JAAS_OPTIONS);
  /**
   * This app login module allows a user name provided by application to be specified.
   */
  private static final AppConfigurationEntry APP_LOGIN = new AppConfigurationEntry(
      AppLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
      EMPTY_JAAS_OPTIONS);

  private static final AppConfigurationEntry TACHYON_LOGIN = new AppConfigurationEntry(
      TachyonLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
      EMPTY_JAAS_OPTIONS);

  // TODO(dong): add Kerberos_LOGIN module
  // private static final AppConfigurationEntry KERBEROS_LOGIN = ...

  /**
   * In SIMPLE mode, JAAS first tries to retrieve the user name set by the application with
   * {@link tachyon.security.login.AppLoginModule}. Upon failure, it uses the OS specific login
   * module to fetch the OS user, and then uses the
   * {@link tachyon.security.login .TachyonLoginModule} to convert it to a Tachyon user represented
   * by {@link tachyon.security.User}. In CUSTOM mode, we also use this configuration.
   */
  private static final AppConfigurationEntry[] SIMPLE = new AppConfigurationEntry[] {APP_LOGIN,
      OS_SPECIFIC_LOGIN, TACHYON_LOGIN};

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
