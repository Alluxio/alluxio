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
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.authentication.AuthenticationFactory;
import tachyon.security.login.TachyonJaasConfiguration;

/**
 * A Singleton of LoginUser, which is an instance of {@link tachyon.security.User}. It represents
 * the user of Tachyon client, when connecting to Tachyon service.
 *
 * The implementation of getting a login user supports Windows, Unix, and Kerberos login modules.
 *
 * This singleton uses lazy initialization.
 */
public final class LoginUser {

  /** User instance of the login user in Tachyon client process */
  private static User sLoginUser;

  /**
   * This constructor should never be invoked.
   */
  private LoginUser() {} // prevent instantiation

  /**
   * Get current singleton login user. This method is called to identify the singleton user who runs
   * Tachyon client. When Tachyon client gets a user by this method and connects to Tachyon service,
   * this user represents the client and is maintained in service.
   *
   * @param conf Tachyon configuration
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  public static User get(TachyonConf conf) throws IOException {
    if (sLoginUser == null) {
      synchronized (LoginUser.class) {
        if (sLoginUser == null) {
          sLoginUser = login(conf);
        }
      }
    }
    return sLoginUser;
  }

  /**
   * Login based on the LoginModules.
   *
   * @param conf Tachyon configuration
   * @return the login user
   * @throws IOException if login fails
   */
  private static User login(TachyonConf conf) throws IOException {
    AuthenticationFactory.AuthType authType = conf.getEnum(
        Constants.TACHYON_SECURITY_AUTHENTICATION, AuthenticationFactory.AuthType.class);
    checkSecurityEnabled(authType);

    try {
      Subject subject = new Subject();

      LoginContext loginContext =
          new LoginContext(authType.getAuthName(), subject, null, new TachyonJaasConfiguration());
      loginContext.login();

      Set<User> userSet = subject.getPrincipals(User.class);
      if (!userSet.isEmpty()) {
        if (userSet.size() == 1) {
          return userSet.iterator().next();
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

  /**
   * Check whether Tachyon is running in secure mode, such as SIMPLE, KERBEROS, CUSTOM.
   *
   * @param authType the authentication type in configuration
   */
  private static void checkSecurityEnabled(AuthenticationFactory.AuthType authType) {
    // TODO: add Kerberos condition check.
    if (authType != AuthenticationFactory.AuthType.SIMPLE
        && authType != AuthenticationFactory.AuthType.CUSTOM) {
      throw new UnsupportedOperationException(
          "User is not supported in " + authType.getAuthName() + " mode");
    }
  }
}
