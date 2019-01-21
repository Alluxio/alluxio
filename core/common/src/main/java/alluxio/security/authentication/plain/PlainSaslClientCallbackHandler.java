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

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * A client side callback to put application provided username/password into SASL transport.
 */
public final class PlainSaslClientCallbackHandler implements CallbackHandler {

  private final String mUserName;
  private final String mPassword;

  /**
   * Constructs a new client side callback.
   *
   * @param userName the name of the user
   * @param password the password
   */
  public PlainSaslClientCallbackHandler(String userName, String password) {
    mUserName = userName;
    mPassword = password;
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        NameCallback nameCallback = (NameCallback) callback;
        nameCallback.setName(mUserName);
      } else if (callback instanceof PasswordCallback) {
        PasswordCallback passCallback = (PasswordCallback) callback;
        passCallback.setPassword(mPassword == null ? null : mPassword.toCharArray());
      } else {
        Class<?> callbackClass = (callback == null) ? null : callback.getClass();
        throw new UnsupportedCallbackException(callback, callbackClass + " is unsupported.");
      }
    }
  }
}
