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

import alluxio.security.authentication.plain.PlainSaslServer;
import alluxio.security.authentication.plain.PlainSaslServerProvider;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Tests the {@link PlainSaslServer} class.
 */
public final class PlainSaslServerTest {
  private static byte sSEPARATOR = 0x00; // US-ASCII <NUL>
  private SaslServer mPlainSaslServer = null;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the server before a test runs.
   */
  @Before
  public void before() throws Exception {
    mPlainSaslServer = new PlainSaslServer.Factory().createSaslServer(
        PlainSaslServerProvider.MECHANISM, null, null, null, new MockCallbackHandler());
  }

  /**
   * Simulate the authentication data included user information from client side.
   *
   * @param user The name of the user will be transferred from client side
   * @param password The password will be transferred from client side
   * @return The response is simulated from the client side
   */
  private byte[] getUserInfo(String user, String password) throws Exception {
    byte[] auth = user.getBytes("UTF8");
    byte[] pw = password.getBytes("UTF8");
    byte[] result = new byte[pw.length + auth.length + 2];
    int pos = 0;
    result[pos++] = sSEPARATOR;
    System.arraycopy(auth, 0, result, pos, auth.length);

    pos += auth.length;
    result[pos++] = sSEPARATOR;

    System.arraycopy(pw, 0, result, pos, pw.length);
    return result;
  }

  /**
   * Tests the {@link PlainSaslServer#evaluateResponse(byte[])} method when the user is not set.
   */
  @Test
  public void userIsNotSet() throws Exception {
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("Plain authentication failed: No authentication identity provided");
    mPlainSaslServer.evaluateResponse(getUserInfo("", "anonymous"));
  }

  /**
   * Tests the {@link PlainSaslServer#evaluateResponse(byte[])} method when the password is not set.
   */
  @Test
  public void passwordIsNotSet() throws Exception {
    mThrown.expect(SaslException.class);
    mThrown.expectMessage("Plain authentication failed: No password provided");
    mPlainSaslServer.evaluateResponse(getUserInfo("alluxio", ""));
  }

  /**
   * Tests the {@link PlainSaslServer#getAuthorizationID()} method.
   */
  @Test
  public void authenticationNotComplete() {
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage("PLAIN authentication not completed");
    mPlainSaslServer.getAuthorizationID();
  }

  /**
   * Tests the {@link PlainSaslServer#getAuthorizationID()} to retrieve the correct user.
   */
  @Test
  public void userPasswordReceive() throws Exception {
    String testUser = "alluxio";
    String password = "anonymous";
    mPlainSaslServer.evaluateResponse(getUserInfo(testUser, password));
    assertEquals(testUser, mPlainSaslServer.getAuthorizationID());
  }

  /**
   * A server side callback that is authorized.
   */
  private static class MockCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        }
      }
      ac.setAuthorized(true);
    }
  }

  /**
   * A server side callback that is not authorized.
   */
  private static class MockCallbackHandlerUnauthorized implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        }
      }
      ac.setAuthorized(false);
    }
  }

  /**
   * Tests the {@link PlainSaslServer#evaluateResponse(byte[])} method when AuthorizeCallback is
   * not authorized.
   */
  @Test
  public void unauthorizedCallback() throws Exception {
    String testUser = "alluxio";
    String password = "anonymous";
    mPlainSaslServer = new PlainSaslServer.Factory().createSaslServer(
        PlainSaslServerProvider.MECHANISM, null, null, null, new MockCallbackHandlerUnauthorized());

    mThrown.expect(SaslException.class);
    mThrown.expectMessage("Plain authentication failed: AuthorizeCallback authorized failure");
    mPlainSaslServer.evaluateResponse(getUserInfo(testUser, password));
  }
}
