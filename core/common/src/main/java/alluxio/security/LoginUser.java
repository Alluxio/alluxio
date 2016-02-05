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

package alluxio.security;

import java.io.IOException;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.security.authentication.AuthType;
import alluxio.security.login.AppLoginModule;
import alluxio.security.login.LoginModuleConfiguration;

/**
 * A Singleton of LoginUser, which is an instance of {@link alluxio.security.User}. It represents
 * the user of Alluxio client, when connecting to Alluxio service.
 *
 * The implementation of getting a login user supports Windows, Unix, and Kerberos login modules.
 *
 * This singleton uses lazy initialization.
 */
@ThreadSafe
public final class LoginUser {

  /** User instance of the login user in Alluxio client process */
  private static User sLoginUser;

  private LoginUser() {} // prevent instantiation

  /**
   * Gets current singleton login user. This method is called to identify the singleton user who
   * runs Alluxio client. When Alluxio client gets a user by this method and connects to Alluxio
   * service, this user represents the client and is maintained in service.
   *
   * @param conf Alluxio configuration
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  public static User get(Configuration conf) throws IOException {
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
   * Logs in based on the LoginModules.
   *
   * @param conf Alluxio configuration
   * @return the login user
   * @throws IOException if login fails
   */
  private static User login(Configuration conf) throws IOException {
    AuthType authType = conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    checkSecurityEnabled(authType);

    try {
      Subject subject = new Subject();

      CallbackHandler callbackHandler = null;
      if (authType.equals(AuthType.SIMPLE) || authType.equals(AuthType.CUSTOM)) {
        callbackHandler = new AppLoginModule.AppCallbackHandler(conf);
      }

      // Create LoginContext based on authType, corresponding LoginModule should be registered
      // under the authType name in LoginModuleConfiguration.
      LoginContext loginContext =
          new LoginContext(authType.getAuthName(), subject, callbackHandler,
              new LoginModuleConfiguration());
      loginContext.login();

      Set<User> userSet = subject.getPrincipals(User.class);
      if (userSet.isEmpty()) {
        throw new LoginException("No Alluxio User is found.");
      }
      if (userSet.size() > 1) {
        throw new LoginException("More than one Alluxio User is found");
      }
      return userSet.iterator().next();
    } catch (LoginException e) {
      throw new IOException("Fail to login", e);
    }
  }

  /**
   * Checks whether Alluxio is running in secure mode, such as {@link AuthType#SIMPLE},
   * {@link AuthType#KERBEROS}, {@link AuthType#CUSTOM}.
   *
   * @param authType the authentication type in configuration
   */
  private static void checkSecurityEnabled(AuthType authType) {
    // TODO(dong): add Kerberos condition check.
    if (authType != AuthType.SIMPLE && authType != AuthType.CUSTOM) {
      throw new UnsupportedOperationException("User is not supported in " + authType.getAuthName()
          + " mode");
    }
  }
}
