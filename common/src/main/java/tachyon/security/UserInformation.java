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
import java.util.Set;

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
 * This class provides methods to determine the login user and connected remote client users.
 * When fetching a login user of Tachyon service or Tachyon client, it supports Windows, Unix,
 * and Kerberos login modules.
 * (TODO) When creating a client user, it instantiates a {@link tachyon.security.User} by the user
 * name transmitted by SASL transport from client side.
 */
public class UserInformation {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The configuration to use */
  private static final TachyonConf TACHYON_CONF;
  /** The authentication method to use. */
  private static AuthenticationFactory.AuthType sAuthType;
  /** User instance of the login user in Tachyon process (Tachyon service or Tachyon client) */
  private static User sLoginUser;

  static {
    TACHYON_CONF = new TachyonConf();
    sAuthType = AuthenticationFactory.getAuthTypeFromConf(TACHYON_CONF);
  }

  /**
   * Check whether Tachyon is running in secure mode, such as SIMPLE, KERBEROS.
   */
  private static void isSecurityEnabled() {
    //TODO: add Kerberos condition check.
    if (sAuthType != AuthenticationFactory.AuthType.SIMPLE) {
      throw new UnsupportedOperationException("UserInformation is only supported in SIMPLE mode");
    }
  }

  /**
   * Get current login user.
   * This method is called to identify the user who runs Tachyon service or Tachyon client.
   * When Tachyon client gets a user by this method and connects to Tachyon service,
   * this user represents the client and is maintained in service.
   * @return the login user
   * @throws IOException if login fails
   */
  public static User getTachyonLoginUser() throws IOException {
    if (sLoginUser == null) {
      login();
    }
    return sLoginUser;
  }

  /**
   * Login based on the LoginModules
   * @throws IOException if login fails
   */
  private static void login() throws IOException {
    isSecurityEnabled();
    try {
      Subject subject = new Subject();

      LoginContext loginContext = new LoginContext(sAuthType.getAuthName(), subject, null,
          new TachyonJaasConfiguration());
      loginContext.login();

      Set<User> userSet = subject.getPrincipals(User.class);
      if (!userSet.isEmpty()) {
        if (userSet.size() == 1) {
          sLoginUser = userSet.iterator().next();
        } else {
          throw new LoginException("More than one Tachyon User is found");
        }
      } else {
        throw new LoginException("No Tachyon User is found.");
      }
    } catch (LoginException e) {
      throw new IOException("Fail to login", e);
    }
  }

  @VisibleForTesting
  static void reset() {
    sLoginUser = null;
  }

  @VisibleForTesting
  static void setsAuthType(AuthenticationFactory.AuthType authType) {
    sAuthType = authType;
  }

  // TODO: Create a remote client user in RPC, whose name is transmitted by SASL transport.
  // public static User createRemoteUser(String userName);
}
