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

import alluxio.Configuration;
import alluxio.Constants;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;

/**
 * Tests the {@link PlainSaslServerCallbackHandler} class.
 */
public class PlainSaslServerCallbackHandlerTest {
  private Configuration mConf;
  private CallbackHandler mPlainServerCBHandler;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the configuration and callback handler before running a test.
   *
   * @throws Exception thrown if the authentication provider cannot be set up
   */
  @Before
  public void before() throws Exception {
    mConf = new Configuration();
    mConf.set(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER,
        NameMatchAuthenticationProvider.class.getName());
    mPlainServerCBHandler = new PlainSaslServerCallbackHandler(
        AuthenticationProvider.Factory.create(AuthType.CUSTOM, mConf));
  }

  /**
   * Tests that the authentication callbacks matches.
   *
   * @throws Exception thrown if the handler fails
   */
  @Test
  public void authenticateNameMatchTest() throws Exception {
    String authenticateId = "alluxio-1";
    NameCallback ncb = new NameCallback(" authentication id: ");
    ncb.setName(authenticateId);

    PasswordCallback pcb = new PasswordCallback(" password: ", false);
    pcb.setPassword("password".toCharArray());

    Callback[] callbacks = new Callback[]{ncb, pcb,
        new AuthorizeCallback(authenticateId, authenticateId)};
    mPlainServerCBHandler.handle(callbacks);
  }

  /**
   * Tests that the authentication callbacks do not match.
   */
  @Test
  public void authenticateNameNotMatchTest() throws Exception {
    mThrown.expect(AuthenticationException.class);
    mThrown.expectMessage("Only allow the user starting with alluxio");

    String authenticateId = "not-alluxio-1";
    NameCallback ncb = new NameCallback(" authentication id: ");
    ncb.setName(authenticateId);

    PasswordCallback pcb = new PasswordCallback(" password: ", false);
    pcb.setPassword("password".toCharArray());

    Callback[] callbacks = new Callback[]{ncb, pcb,
        new AuthorizeCallback(authenticateId, authenticateId)};
    mPlainServerCBHandler.handle(callbacks);
  }

  /**
   * Tests that the incorrect password should fail the authentication.
   */
  @Test
  public void authenticateCorrectPasswordTest() throws Exception {
    mThrown.expect(AuthenticationException.class);
    mThrown.expectMessage("Wrong password");

    String authenticateId = "alluxio-1";
    NameCallback ncb = new NameCallback(" authentication id: ");
    ncb.setName(authenticateId);

    PasswordCallback pcb = new PasswordCallback(" password: ", false);
    pcb.setPassword("not-password".toCharArray());

    Callback[] callbacks = new Callback[]{ncb, pcb,
        new AuthorizeCallback(authenticateId, authenticateId)};
    mPlainServerCBHandler.handle(callbacks);
  }

  /**
   * An {@link AuthenticationProvider} that only allows users starting with alluxio, password
   * should be "password".
   */
  public static class NameMatchAuthenticationProvider implements AuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.matches("^alluxio.*")) {
        throw new AuthenticationException("Only allow the user starting with alluxio");
      }
      if (!password.matches("^password")) {
        throw new AuthenticationException("Wrong password");
      }
    }
  }
}
