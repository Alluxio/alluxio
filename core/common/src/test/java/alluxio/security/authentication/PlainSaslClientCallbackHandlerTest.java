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

package alluxio.security.authentication;

import static org.junit.Assert.assertEquals;

import alluxio.security.authentication.plain.PlainSaslClientCallbackHandler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;

/**
 * Tests the {@link PlainSaslClientCallbackHandler}
 * class.
 */
public final class PlainSaslClientCallbackHandlerTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Tests that the callback is handled correctly.
   */
  @Test
  public void clientCallbackHandler() throws Exception {
    Callback[] callbacks = new Callback[2];
    callbacks[0] = new NameCallback("Username:");
    callbacks[1] = new PasswordCallback("Password:", true);

    String user = "alluxio-user-1";
    String password = "alluxio-user-1-password";

    CallbackHandler clientCBHandler =
        new PlainSaslClientCallbackHandler(user, password);
    clientCBHandler.handle(callbacks);

    validateCallbacks(user, password, callbacks);
  }

  /**
   * Tests that an exception is thrown in case an unsupported callback is used.
   */
  @Test
  public void unsupportCallback() throws Exception {
    mThrown.expect(UnsupportedCallbackException.class);
    mThrown.expectMessage(RealmCallback.class + " is unsupported.");

    Callback[] callbacks = new Callback[3];
    callbacks[0] = new NameCallback("Username:");
    callbacks[1] = new PasswordCallback("Password:", true);
    callbacks[2] = new RealmCallback("Realm:");

    String user = "alluxio-user-2";
    String password = "alluxio-user-2-password";
    CallbackHandler clientCBHandler =
        new PlainSaslClientCallbackHandler(user, password);
    clientCBHandler.handle(callbacks);
  }

  /**
   * Tests that the callback can handle a non-existent user.
   */
  @Test
  public void nullNameCallback() throws Exception {
    Callback[] callbacks = new Callback[2];
    callbacks[0] = new NameCallback("Username:");
    callbacks[1] = new PasswordCallback("Password:", true);

    String user = null;
    String password = "alluxio-user-3-password";
    CallbackHandler clientCBHandler =
        new PlainSaslClientCallbackHandler(user, password);
    clientCBHandler.handle(callbacks);

    validateCallbacks(user, password, callbacks);
  }

  /**
   * Tests that the callback can handle a non-existent password.
   */
  @Test
  public void nullPasswordCallback() throws Exception {
    Callback[] callbacks = new Callback[2];
    callbacks[0] = new NameCallback("Username:");
    callbacks[1] = new PasswordCallback("Password:", true);

    String user = "alluxio-user-4";
    String password = null;
    CallbackHandler clientCBHandler =
        new PlainSaslClientCallbackHandler(user, password);
    clientCBHandler.handle(callbacks);

    validateCallbacks(user, password, callbacks);
  }

  /**
   * Tests that an exception is thrown when a callback is {@code null}.
   */
  @Test
  public void nullCallback() throws Exception {
    mThrown.expect(UnsupportedCallbackException.class);
    mThrown.expectMessage(null + " is unsupported.");

    Callback[] callbacks = new Callback[3];
    callbacks[0] = new NameCallback("Username:");
    callbacks[1] = new PasswordCallback("Password:", true);
    callbacks[2] = null;

    String user = "alluxio-user-5";
    String password = "alluxio-user-5-password";
    CallbackHandler clientCBHandler =
        new PlainSaslClientCallbackHandler(user, password);
    clientCBHandler.handle(callbacks);
  }

  private void validateCallbacks(String user, String passwd, Callback[] callbacks)
      throws IOException, UnsupportedCallbackException {
    for (Callback cb : callbacks) {
      if (cb instanceof NameCallback) {
        assertEquals(user, ((NameCallback) cb).getName());
      } else if (cb instanceof PasswordCallback) {
        char[] passwordChar = ((PasswordCallback) cb).getPassword();
        assertEquals(passwd, passwordChar == null ? null : String.copyValueOf(passwordChar));
      }
    }
  }
}
