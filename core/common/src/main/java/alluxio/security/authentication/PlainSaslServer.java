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

package alluxio.security.authentication;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import com.google.common.base.Preconditions;

import alluxio.Configuration;
import alluxio.exception.ExceptionMessage;
import alluxio.security.User;
import alluxio.util.SecurityUtils;

/**
 * This class provides PLAIN SASL authentication.
 *
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism. There is a new
 * provider needed to register to support server-side PLAIN mechanism. This class completes three
 * basic steps to implement a SASL security provider:
 * <ol>
 * <li>Write a class that implements the {@link SaslServer} interface</li>
 * <li>Write a factory class implements the {@link javax.security.sasl.SaslServerFactory}</li>
 * <li>Write a JCA provider that registers the factory</li>
 * </ol>
 *
 * NOTE: When this SaslServer works on authentication (i.e., in the method
 * {@link #evaluateResponse(byte[])}, it always assigns authentication ID to authorization ID
 * currently.
 */
@NotThreadSafe
// TODO(dong): Authorization ID and authentication ID could be different after supporting
// impersonation.
public final class PlainSaslServer implements SaslServer {
  /**
   * This ID represent the authorized client user, who has been authenticated successfully. It is
   * associated with the client connection thread for following action authorization usage.
   */
  private String mAuthorizationId;
  /** Whether an authentication is complete or not. */
  private boolean mCompleted;
  private CallbackHandler mHandler;

  PlainSaslServer(CallbackHandler handler) throws SaslException {
    mCompleted = false;
    mHandler = handler;
  }

  @Override
  public String getMechanismName() {
    return PlainSaslServerProvider.MECHANISM;
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    Preconditions.checkState(!mCompleted, "PLAIN authentication has completed");
    Preconditions.checkArgument(response != null, "Received null response");

    try {
      // parse the response
      // message = [authorizationId] UTF8NUL authenticationId UTF8NUL passwd'
      // authorizationId may be empty,then the authorizationId = authenticationId
      String payload;
      try {
        payload = new String(response, "UTF-8");
      } catch (Exception e) {
        throw new IllegalArgumentException("Received corrupt response", e);
      }
      String[] parts = payload.split("\u0000", 3);
      // validate response
      if (parts.length != 3) {
        throw new IllegalArgumentException("Invalid message format, parts must contain 3 items");
      }
      String authorizationId = parts[0];
      String authenticationId = parts[1];
      String passwd = parts[2];
      Preconditions.checkState(authenticationId != null && !authenticationId.isEmpty(),
          "No authentication identity provided");
      Preconditions.checkState(passwd != null && !passwd.isEmpty(), "No password provided");

      if (authorizationId == null || authorizationId.isEmpty()) {
        authorizationId = authenticationId;
      } else if (!authorizationId.equals(authenticationId)) {
        // TODO(dong): support impersonation
        throw new UnsupportedOperationException("Impersonation is not supported now.");
      }

      NameCallback nameCallback = new NameCallback("User");
      nameCallback.setName(authenticationId);
      PasswordCallback passwordCallback = new PasswordCallback("Password", false);
      passwordCallback.setPassword(passwd.toCharArray());
      AuthorizeCallback authCallback = new AuthorizeCallback(authenticationId, authorizationId);

      Callback[] cbList = {nameCallback, passwordCallback, authCallback};
      mHandler.handle(cbList);
      if (!authCallback.isAuthorized()) {
        throw new SaslException("AuthorizeCallback authorized failure");
      }
      mAuthorizationId = authCallback.getAuthorizedID();
    } catch (Exception e) {
      throw new SaslException("Plain authentication failed: " + e.getMessage(), e);
    }
    mCompleted = true;
    return null;
  }

  @Override
  public boolean isComplete() {
    return mCompleted;
  }

  @Override
  public String getAuthorizationID() {
    checkNotComplete();
    return mAuthorizationId;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) {
    throw new UnsupportedOperationException("PLAIN doesn't support wrap or unwrap operation");
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) {
    throw new UnsupportedOperationException("PLAIN doesn't support wrap or unwrap operation");
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    checkNotComplete();
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }

  @Override
  public void dispose() {
    if (mCompleted) {
      // clean up the user in threadlocal, when client connection is closed.
      AuthorizedClientUser.remove();
    }

    mCompleted = false;
    mHandler = null;
    mAuthorizationId = null;
  }

  private void checkNotComplete() {
    if (!mCompleted) {
      throw new IllegalStateException("PLAIN authentication not completed");
    }
  }

  /**
   * {@link PlainServerCallbackHandler} is used by the SASL mechanisms to get further information to
   * complete the authentication. For example, a SASL mechanism might use this callback handler to
   * do verification operation.
   */
  public static final class PlainServerCallbackHandler implements CallbackHandler {
    private final AuthenticationProvider mAuthenticationProvider;

    /**
     * Constructs a new callback handler.
     *
     * @param authenticationProvider the authentication provider used
     */
    public PlainServerCallbackHandler(AuthenticationProvider authenticationProvider) {
      mAuthenticationProvider = authenticationProvider;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      String username = null;
      String password = null;
      AuthorizeCallback ac = null;

      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          username = nc.getName();
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          password = new String(pc.getPassword());
        } else if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unsupport callback");
        }
      }

      mAuthenticationProvider.authenticate(username, password);

      if (ac != null) {
        ac.setAuthorized(true);

        // After verification succeeds, a user with this authz id will be set to a Threadlocal.
        AuthorizedClientUser.set(ac.getAuthorizedID());
      }
    }
  }

  /**
   * An instance of this class represents a client user connecting to Alluxio service.
   *
   * It is maintained in a {@link ThreadLocal} variable based on the Thrift RPC mechanism.
   * {@link org.apache.thrift.server.TThreadPoolServer} allocates a thread to serve a connection
   * from client side and take back it when connection is closed. During the thread alive cycle,
   * all the RPC happens in this thread. These RPC methods implemented in server side could
   * get the client user by this class.
   */
  public static final class AuthorizedClientUser {

    /**
     * A {@link ThreadLocal} variable to maintain the client user along with a specific thread.
     */
    private static ThreadLocal<User> sUserThreadLocal = new ThreadLocal<User>();

    /**
     * Creates a {@link User} and sets it to the {@link ThreadLocal} variable.
     *
     * @param userName the name of the client user
     */
    public static void set(String userName) {
      sUserThreadLocal.set(new User(userName));
    }

    /**
     * Gets the {@link User} from the {@link ThreadLocal} variable.
     *
     * @param conf the runtime configuration of Alluxio Master
     * @return the client user
     * @throws IOException if authentication is not enabled
     */
    public static User get(Configuration conf) throws IOException {
      if (!SecurityUtils.isAuthenticationEnabled(conf)) {
        throw new IOException(ExceptionMessage.AUTHENTICATION_IS_NOT_ENABLED.getMessage());
      }
      return sUserThreadLocal.get();
    }

    /**
     * Removes the {@link User} from the {@link ThreadLocal} variable.
     */
    public static void remove() {
      sUserThreadLocal.remove();
    }
  }
}
