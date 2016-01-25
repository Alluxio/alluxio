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

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import tachyon.Constants;
import tachyon.security.User;

/**
 * An app login module that creates a user based on the user name provided through application
 * configuration. Specifically, through Java system property tachyon.security.login.username. This
 * module is useful if multiple Tachyon clients running under same OS user name want to get
 * different identifies (for resource and data management), or if Tachyon clients running under
 * different OS user names want to get same identify.
 */
@NotThreadSafe
public final class AppLoginModule implements LoginModule {
  private Subject mSubject;
  private User mUser;
  private CallbackHandler mCallbackHandler;

  @Override
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    mSubject = subject;
    mCallbackHandler = callbackHandler;
  }

  /**
   * Retrieves the user name by querying the property of
   * {@link Constants#SECURITY_LOGIN_USERNAME} through {@link AppCallbackHandler}.
   *
   * @return true if user name provided by application is set and not empty
   * @throws LoginException when the login fails
   */
  @Override
  public boolean login() throws LoginException {
    Callback[] callbacks = new Callback[1];
    callbacks[0] = new NameCallback("user name: ");
    try {
      mCallbackHandler.handle(callbacks);
    } catch (IOException e) {
      throw new LoginException(e.getMessage());
    } catch (UnsupportedCallbackException e) {
      throw new LoginException(e.getMessage());
    }

    String userName = ((NameCallback) callbacks[0]).getName();
    if (!userName.isEmpty()) {
      mUser = new User(userName);
      return true;
    }
    return false;
  }

  /**
   * Aborts the authentication (second phase).
   *
   * <p>
   * This method is called if the LoginContext's overall authentication failed. (login failed) It
   * cleans up any state that was changed in the login and commit methods.
   *
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
   * <p>
   * This method is called if the LoginContext's overall authentication succeeded. The
   * implementation first checks if there is already Tachyon user in the subject. If not, it adds
   * the previously logged in Tachyon user into the subject.
   *
   * @return true if a Tachyon user if found or created
   * @throws LoginException not Tachyon user is found or created
   */
  @Override
  public boolean commit() throws LoginException {
    // if there is already a Tachyon user, it's done.
    if (!mSubject.getPrincipals(User.class).isEmpty()) {
      return true;
    }
    // add the logged in user into subject
    if (mUser != null) {
      mSubject.getPrincipals().add(mUser);
      return true;
    }
    // throw exception if no Tachyon user is found or created.
    throw new LoginException("Cannot find a user");
  }

  /**
   * Logs out the user
   *
   * <p>
   * The implementation removes the User associated with the Subject.
   *
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
}
