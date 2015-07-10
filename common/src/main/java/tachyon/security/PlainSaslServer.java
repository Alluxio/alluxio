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
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Because the Java SunSASL provider doesn’t support the server-side PLAIN mechanism.
 * There is a new provider needed to register to support server-side PLAIN mechanism.
 * There are three basic steps in implementing a SASL security provider:
 * 1.Write a class that implements the SaslServer interface
 * 2.Write a factory class implements the SaslServerFactory
 * 3.Write a JCA provider that registers the factory
 */
public class PlainSaslServer implements SaslServer {
  private String mUser;
  private final CallbackHandler mHandler;

  PlainSaslServer(CallbackHandler mHandler, String authMethodStr) throws SaslException {
    this.mHandler = mHandler;
  }

  @Override
  public String getMechanismName() {
    return "PLAIN";
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    try {
      // parse the response
      // message   = [authzid] UTF8NUL authcid UTF8NUL passwd'

      Deque<String> tokenList = new ArrayDeque<String>();
      StringBuilder messageToken = new StringBuilder();
      for (byte b : response) {
        if (b == 0) {
          tokenList.addLast(messageToken.toString());
          messageToken = new StringBuilder();
        } else {
          messageToken.append((char) b);
        }
      }
      tokenList.addLast(messageToken.toString());

      // validate response
      if (tokenList.size() < 2 || tokenList.size() > 3) {
        throw new SaslException("Invalid message format");
      }
      String passwd = tokenList.removeLast();
      mUser = tokenList.removeLast();
      // optional authzid
      String authzId;
      if (tokenList.isEmpty()) {
        authzId = mUser;
      } else {
        authzId = tokenList.removeLast();
      }
      if (mUser == null || mUser.isEmpty()) {
        throw new SaslException("No user name provided");
      }
      if (passwd == null || passwd.isEmpty()) {
        throw new SaslException("No password name provided");
      }

      NameCallback nameCallback = new NameCallback("User");
      nameCallback.setName(mUser);
      PasswordCallback pcCallback = new PasswordCallback("Password", false);
      pcCallback.setPassword(passwd.toCharArray());
      AuthorizeCallback acCallback = new AuthorizeCallback(mUser, authzId);

      Callback[] cbList = {nameCallback, pcCallback, acCallback};
      mHandler.handle(cbList);
      if (!acCallback.isAuthorized()) {
        throw new SaslException("Authentication failed");
      }
    } catch (IllegalStateException eL) {
      throw new SaslException("Invalid message format", eL);
    } catch (IOException eI) {
      throw new SaslException("Error validating the login", eI);
    } catch (UnsupportedCallbackException eU) {
      throw new SaslException("Error validating the login", eU);
    }
    return null;
  }

  @Override
  public boolean isComplete() {
    return mUser != null;
  }

  @Override
  public String getAuthorizationID() {
    return mUser;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    return null;
  }

  @Override
  public void dispose() {
  }
}
