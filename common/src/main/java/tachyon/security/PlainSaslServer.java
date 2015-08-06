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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import tachyon.security.authentication.AuthenticationProvider;

/**
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism.
 * There is a new provider needed to register to support server-side PLAIN mechanism.
 * There are three basic steps in implementing a SASL security provider:
 * 1.Write a class that implements the SaslServer interface
 * 2.Write a factory class implements the SaslServerFactory
 * 3.Write a JCA provider that registers the factory
 */
public class PlainSaslServer implements SaslServer {
  private String mAuthcid;
  private boolean mCompleted;
  private CallbackHandler mHandler;

  PlainSaslServer(CallbackHandler handler) throws SaslException {
    mHandler = handler;
  }

  @Override
  public String getMechanismName() {
    return "PLAIN";
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    if (mCompleted) {
      throw new IllegalStateException("PLAIN authentication has completed");
    }
    if (response == null) {
      throw new IllegalArgumentException("Received null response");
    }
    try {
      // parse the response
      // message   = [authzid] UTF8NUL authcid UTF8NUL passwd'
      // authzid may be empty,then the authzid = authcid
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
      String authzid = parts[0];
      mAuthcid = parts[1];
      String passwd = parts[2];
      if (mAuthcid == null || mAuthcid.isEmpty()) {
        throw new IllegalStateException("No authentication identity provided");
      }
      if (passwd == null || passwd.isEmpty()) {
        throw new IllegalStateException("No password provided");
      }
      if (authzid == null || authzid.isEmpty()) { // authzid = authcid
        authzid = mAuthcid;
      }

      NameCallback nameCallback = new NameCallback("User");
      nameCallback.setName(mAuthcid);
      PasswordCallback passwordCallback = new PasswordCallback("Password", false);
      passwordCallback.setPassword(passwd.toCharArray());
      AuthorizeCallback authCallback = new AuthorizeCallback(mAuthcid, authzid);

      Callback[] cbList = {nameCallback, passwordCallback, authCallback};
      mHandler.handle(cbList);
      if (!authCallback.isAuthorized()) {
        throw new SaslException("AuthorizeCallback authorized failure");
      }
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
    throwIfNotComplete();
    return mAuthcid;
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
    throwIfNotComplete();
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }

  @Override
  public void dispose() {
    mCompleted = false;
    mHandler = null;
    mAuthcid = null;
  }

  private void throwIfNotComplete() {
    if (!mCompleted) {
      throw new IllegalStateException("PLAIN authentication not completed");
    }
  }

  /**
   * PlainServerCallbackHandler is used by the SASL mechanisms to get further information
   * to complete the authentication. For example, a SASL mechanism might use this callback handler
   * to do verify operation.
   */
  public static final class PlainServerCallbackHandler implements CallbackHandler {
    private final AuthenticationProvider mAuthenticationPrivoder;

    public PlainServerCallbackHandler(AuthenticationProvider authenticationProvider) {
      mAuthenticationPrivoder = authenticationProvider;
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

      mAuthenticationPrivoder.authenticate(username, password);

      if (ac != null) {
        ac.setAuthorized(true);
      }
    }
  }
}
