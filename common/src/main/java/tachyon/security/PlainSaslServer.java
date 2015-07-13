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
import java.util.ArrayDeque;
import java.util.Deque;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism.
 * There is a new provider needed to register to support server-side PLAIN mechanism.
 * There are three basic steps in implementing a SASL security provider:
 * 1.Write a class that implements the SaslServer interface
 * 2.Write a factory class implements the SaslServerFactory
 * 3.Write a JCA provider that registers the factory
 */
public class PlainSaslServer implements SaslServer {
  private String authcid;
  private boolean completed;
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
    if (completed) {
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
        throw new IllegalArgumentException("Received corrupt response");
      }
      if (parts[0].isEmpty()) { // authzid = authcid
        parts[0] = parts[1];
      }
      String passwd = parts[2];
      authcid = parts[1];
      if (authcid == null || authcid.isEmpty()) {
        throw new IllegalStateException("No authentication identity provided");
      }
      if (passwd == null || passwd.isEmpty()) {
        throw new IllegalStateException("No password provided");
      }

      NameCallback nameCallback = new NameCallback("User");
      nameCallback.setName(authcid);
      PasswordCallback passwordCallback = new PasswordCallback("Password", false);
      passwordCallback.setPassword(passwd.toCharArray());
      AuthorizeCallback authCallback = new AuthorizeCallback(authcid, parts[0]);

      Callback[] cbList = {nameCallback, passwordCallback, authCallback};
      mHandler.handle(cbList);
      if (!authCallback.isAuthorized()) {
        throw new SaslException("Authentication failed");
      }
    } catch (IOException ioe) {
      throw new SaslException("Error validating the login", ioe);
    } catch (UnsupportedCallbackException uce) {
      throw new SaslException("Error validating the login", uce);
    }
    completed = true;
    return null;
  }

  @Override
  public boolean isComplete() {
    return completed;
  }

  @Override
  public String getAuthorizationID() {
    throwIfNotComplete();
    return authcid;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) {
    throwIfNotComplete();
    throw new IllegalStateException("PLAIN supports neither integrity nor privacy");
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) {
    throwIfNotComplete();
    throw new IllegalStateException("PLAIN supports neither integrity nor privacy");
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    throwIfNotComplete();
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }

  @Override
  public void dispose() {
    completed = false;
    mHandler = null;
    authcid = null;
  }

  private void throwIfNotComplete() {
    if (!completed) {
      throw new IllegalStateException("PLAIN authentication not completed");
    }
  }
}