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

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * A login module that search the Kerberos or OS user from Subject, and then convert to a Tachyon
 * user. It does not really authenticate the user in its login method.
 */
public class TachyonLoginModule implements LoginModule {
  private Subject mSubject;
  private User mUser;

  @Override
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    mSubject = subject;
  }

  /**
   * Authenticate the user (first phase).
   *
   * The implementation does not really authenticate the user here. Always return true.
   * @return true in all cases
   * @throws javax.security.auth.login.LoginException
   */
  @Override
  public boolean login() throws LoginException {
    return true;
  }

  /**
   * Abort the authentication (second phase).
   *
   * This method is called if the LoginContext's overall authentication failed. (login failed)
   * Since we do nothing in login method and no internal states need to be cleaned,
   * the implementation here is just returning true.
   * @return true in all cases
   * @throws LoginException
   */
  @Override
  public boolean abort() throws LoginException {
    return true;
  }

  /**
   * Commit the authentication (second phase).
   *
   * This method is called if the LoginContext's overall authentication succeeded. (login
   * succeeded)
   * The implementation searches the Kerberos or OS user in the Subject. If existed,
   * convert it to a Tachyon user and add into the Subject.
   * @return true in all cases
   * @throws LoginException if the user extending a specific Principal is not found.
   */
  @Override
  public boolean commit() throws LoginException {
    // if there is already a Tachyon user, it's done.
    if (!mSubject.getPrincipals(User.class).isEmpty()) {
      return true;
    }

    Principal user = null;

    // TODO: get a Kerberos user if we are using Kerberos.
    // user = getKerberosUser();

    // get a OS user
    if (user == null) {
      user = getPrincipalUser(UserInformation.getOsPrincipalStr());
    }

    // if a user is found, convert it to a Tachyon user and save it.
    if (user != null) {
      mUser = new User(user.getName());
      mSubject.getPrincipals().add(mUser);
      return true;
    }

    throw new LoginException("Cannot find a user");
  }

  /**
   * Logout the user
   *
   * The implementation removes the User associated with the Subject.
   * @return true in all cases
   * @throws LoginException if logout fails
   */
  @Override
  public boolean logout() throws LoginException {
    if (mSubject.isReadOnly()) {
      throw new LoginException("logout Failed: Subject is Readonly.");
    }

    if (mUser != null) {
      mSubject.getPrincipals().remove(mUser);
    }

    return true;
  }

  private Principal getPrincipalUser(String className) throws LoginException {
    // load the principal class
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = ClassLoader.getSystemClassLoader();
    }

    Class<? extends Principal> clazz = null;
    try {
      clazz = (Class<? extends Principal>) loader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new LoginException("Unable to find JAAS principal class:" + e.getMessage());
    }

    // find corresponding user based on the principal
    Set<? extends Principal> userSet = mSubject.getPrincipals(clazz);
    if (!userSet.isEmpty()) {
      if (userSet.size() == 1) {
        return userSet.iterator().next();
      }
      throw new LoginException("More than one instance of Principal " + className + " is found");
    }
    return null;
  }
}
