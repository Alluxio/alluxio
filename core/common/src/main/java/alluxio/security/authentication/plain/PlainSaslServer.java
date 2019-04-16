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

package alluxio.security.authentication.plain;

import alluxio.security.authentication.AuthenticatedClientUser;

import com.google.common.base.Preconditions;

import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * This class provides PLAIN SASL authentication.
 * <p/>
 * NOTE: When this SaslServer works on authentication (i.e., in the method
 * {@link #evaluateResponse(byte[])}, it always assigns authentication ID to authorization ID
 * currently.
 */
@NotThreadSafe
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
  @Nullable
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
  @Nullable
  public Object getNegotiatedProperty(String propName) {
    checkNotComplete();
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }

  @Override
  public void dispose() {
    if (mCompleted) {
      // clean up the user in threadlocal, when client connection is closed.
      AuthenticatedClientUser.remove();
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
   * This class is used to create an instances of {@link PlainSaslServer}. The parameter mechanism
   * must be "PLAIN" when this Factory is called, or null will be returned.
   */
  @ThreadSafe
  public static class Factory implements SaslServerFactory {

    /**
     * Constructs a new {@link Factory} for the {@link PlainSaslServer}.
     */
    public Factory() {}

    /**
     * Creates a {@link SaslServer} using the parameters supplied. It returns null if no SaslServer
     * can be created using the parameters supplied. Throws {@link SaslException} if it cannot
     * create a SaslServer because of an error.
     *
     * @param mechanism the name of a SASL mechanism. (e.g. "PLAIN")
     * @param protocol the non-null string name of the protocol for which the authentication is
     *        being performed
     * @param serverName the non-null fully qualified host name of the server to authenticate to
     * @param props the possibly null set of properties used to select the SASL mechanism and to
     *        configure the authentication exchange of the selected mechanism
     * @param callbackHandler the possibly null callback handler to used by the SASL mechanisms to
     *        do further operation
     * @return A possibly null SaslServer created using the parameters supplied. If null, this
     *         factory cannot produce a SaslServer using the parameters supplied.
     * @exception SaslException If it cannot create a SaslServer because of an error.
     */
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
        Map<String, ?> props, CallbackHandler callbackHandler) throws SaslException {
      Preconditions.checkArgument(PlainSaslServerProvider.MECHANISM.equals(mechanism));
      return new PlainSaslServer(callbackHandler);
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {PlainSaslServerProvider.MECHANISM};
    }
  }
}
