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

import alluxio.conf.PropertyKey;
import alluxio.security.User;

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

/**
 * An app login module that creates a user based on the user name provided through application
 * configuration. Specifically, through Alluxio property alluxio.security.login.username. This
 * module is useful if multiple Alluxio clients running under same OS user name want to get
 * different identifies (for resource and data management), or if Alluxio clients running under
 * different OS user names want to get same identify.
 */
@NotThreadSafe
public final class AppLoginModule implements LoginModule {
  private Subject mSubject;
  private User mUser;
  private CallbackHandler mCallbackHandler;

  /**
   * Constructs a new {@link AppLoginModule}.
   */
  public AppLoginModule() {}

  @Override
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    mSubject = subject;
    mCallbackHandler = callbackHandler;
  }

  /**
   * Retrieves the user name by querying the property of
   * {@link PropertyKey#SECURITY_LOGIN_USERNAME} through {@link AppCallbackHandler}.
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
    } catch (IOException | UnsupportedCallbackException e) {
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
   * implementation first checks if there is already Alluxio user in the subject. If not, it adds
   * the previously logged in Alluxio user into the subject.
   *
   * @return true if an Alluxio user is found or created
   * @throws LoginException not Alluxio user is found or created
   */
  @Override
  public boolean commit() throws LoginException {
    // if there is already an Alluxio user, it's done.
    if (!mSubject.getPrincipals(User.class).isEmpty()) {
      return true;
    }
    // add the logged in user into subject
    if (mUser != null) {
      mSubject.getPrincipals().add(mUser);
      return true;
    }
    // throw exception if no Alluxio user is found or created.
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

  /**
   * A callback handler for {@link AppLoginModule}.
   */
  @NotThreadSafe
  public static final class AppCallbackHandler implements CallbackHandler {
    private final String mUserName;

    /**
     * Creates a new instance of {@link AppCallbackHandler}.
     * @param username the username
     */
    public AppCallbackHandler(String username) {
      mUserName = username;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback) callback;
          nameCallback.setName(mUserName);
        } else {
          Class<?> callbackClass = (callback == null) ? null : callback.getClass();
          throw new UnsupportedCallbackException(callback, callbackClass + " is unsupported.");
        }
      }
    }
  }
}
