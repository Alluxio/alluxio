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

import tachyon.security.authentication.AuthenticationFactory;

/**
 * A JAAS configuration that defines the login modules, by which JAAS uses to login.
 *
 * In implementation, we define several modes (Simple, Kerberos, ...) by constructing different
 * arrays of AppConfigurationEntry, and select the proper array based on the configured mode.
 *
 * Then JAAS login framework use the selected array of AppConfigurationEntry to determine the
 * login modules to be used.
 */
public class TachyonJaasConfiguration extends Configuration {

  private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<String,String>();

  private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
      new AppConfigurationEntry(TachyonJaasProperties.getOsLoginModuleName(),
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);

  private static final AppConfigurationEntry TACHYON_LOGIN =
      new AppConfigurationEntry(TachyonLoginModule.class.getName(),
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, BASIC_JAAS_OPTIONS);

  // TODO: add Kerberos_LOGIN module
  // private static final AppConfigurationEntry KERBEROS_LOGIN = ...

  /**
   * In SIMPLE mode, JAAS use the OS specific login module to fetch the OS user,
   * and then use the {@link tachyon.security.TachyonLoginModule} to convert it to a Tachyon user
   * represented by {@link tachyon.security.User}
   */
  private static final AppConfigurationEntry[] SIMPLE = new
      AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, TACHYON_LOGIN};

  // TODO: add Kerberos mode
  // private static final AppConfigurationEntry[] KERBEROS = ...

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    if (appName.equalsIgnoreCase(AuthenticationFactory.AuthType.SIMPLE.getAuthName())) {
      // TODO: also return SIMPLE for AuthenticationFactory.AuthType.CUSTOM
      return SIMPLE;
    } else if (appName.equalsIgnoreCase(AuthenticationFactory.AuthType.KERBEROS.getAuthName())) {
      // TODO: return KERBEROS;
      throw new UnsupportedOperationException("Kerberos is not supported currently.");
    }
    return null;
  }
}
