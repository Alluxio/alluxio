/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import com.google.common.base.Preconditions;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

/**
 * A callback that is used by the SASL mechanisms to get further information to
 * complete the authentication. For example, a SASL mechanism might use this callback handler to
 * do verification operation.
 */
public final class PlainSaslServerCallbackHandler implements CallbackHandler {
  private final AuthenticationProvider mAuthenticationProvider;

  /**
   * Constructs a new callback handler.
   *
   * @param authenticationProvider the authentication provider used
   */
  public PlainSaslServerCallbackHandler(AuthenticationProvider authenticationProvider) {
    mAuthenticationProvider = Preconditions.checkNotNull(authenticationProvider);
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
      AuthenticatedClientUser.set(ac.getAuthorizedID());
    }
  }
}
