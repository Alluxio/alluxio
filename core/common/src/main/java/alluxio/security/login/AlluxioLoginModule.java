/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.login;

import alluxio.security.User;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * A login module that search the Kerberos or OS user from Subject, and then convert to an Alluxio
 * user. It does not really authenticate the user in its login method.
 */
@NotThreadSafe
public final class AlluxioLoginModule implements LoginModule {
  private Subject mSubject;
  private User mUser;

  /**
   * Constructs a new {@link AlluxioLoginModule}.
   */
  public AlluxioLoginModule() {}

  @Override
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    mSubject = subject;
  }

  /**
   * Authenticates the user (first phase).
   *
   * The implementation does not really authenticate the user here. Always return true.
   * @return true in all cases
   * @throws LoginException when the login fails
   */
  @Override
  public boolean login() throws LoginException {
    return true;
  }

  /**
   * Aborts the authentication (second phase).
   *
   * This method is called if the LoginContext's overall authentication failed. (login failed)
   * It cleans up any state that was changed in the login and commit methods.
   * @return true in all cases
   * @throws LoginException when the abortion fails
   */
  @Override
  public boolean abort() throws LoginException {
    logout();
    mUser = null;
    return true;
  }

  /**
   * Commits the authentication (second phase).
   *
   * This method is called if the LoginContext's overall authentication succeeded. (login
   * succeeded)
   * The implementation searches the Kerberos or OS user in the Subject. If existed,
   * convert it to an Alluxio user and add into the Subject.
   * @return true in all cases
   * @throws LoginException if the user extending a specific Principal is not found
   */
  @Override
  public boolean commit() throws LoginException {
    // if there is already an Alluxio user, it's done.
    if (!mSubject.getPrincipals(User.class).isEmpty()) {
      return true;
    }

    // get a OS user
    Principal user = getPrincipalUser(LoginModuleConfigurationUtils.OS_PRINCIPAL_CLASS_NAME);

    // if a user is found, convert it to an Alluxio user and save it.
    if (user != null) {
      mUser = new User(user.getName());
      mSubject.getPrincipals().add(mUser);
      return true;
    }

    throw new LoginException("Cannot find a user");
  }

  /**
   * Logs out the user.
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

  /**
   * Gets a principal user.
   *
   * @param className the name of class extending Principal
   * @return a user extending a specified Principal
   * @throws LoginException if the specified class can not be found,
   * or there are are more than one instance of Principal
   */
  @Nullable
  private Principal getPrincipalUser(String className) throws LoginException {
    // load the principal class
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = ClassLoader.getSystemClassLoader();
    }

    Class<? extends Principal> clazz;
    try {
      // Declare a temp variable so that we can suppress warnings locally
      @SuppressWarnings("unchecked")
      Class<? extends Principal> tmpClazz =
          (Class<? extends Principal>) loader.loadClass(className);
      clazz = tmpClazz;
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
