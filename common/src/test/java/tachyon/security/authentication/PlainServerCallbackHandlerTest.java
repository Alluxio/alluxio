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

package tachyon.security.authentication;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.authentication.AuthenticationFactory.AuthType;
import tachyon.security.authentication.AuthenticationProvider;
import tachyon.security.authentication.AuthenticationProviderFactory;
import tachyon.security.authentication.PlainSaslServer;

public class PlainServerCallbackHandlerTest {
  private TachyonConf mConf;
  private CallbackHandler mPlainServerCBHandler;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mConf = new TachyonConf();
    mConf.set(Constants.TACHYON_AUTHENTICATION_PROVIDER_CUSTOM_CLASS,
        NameMatchAuthenticationProvider.class.getName());
    mPlainServerCBHandler = new PlainSaslServer.PlainServerCallbackHandler(
        AuthenticationProviderFactory.getAuthenticationProvider(AuthType.CUSTOM, mConf));
  }

  @Test
  public void authenticateNameMatchTest() throws Exception {
    String authenticateId = "tachyon-1";
    NameCallback ncb = new NameCallback(" authentication id: ");
    ncb.setName(authenticateId);

    PasswordCallback pcb = new PasswordCallback(" password: ", false);
    pcb.setPassword("anonymous".toCharArray());

    Callback[] callbacks = new Callback[]{ncb, pcb,
        new AuthorizeCallback(authenticateId, authenticateId)};
    mPlainServerCBHandler.handle(callbacks);
  }

  @Test
  public void authenticateNameNotMatchTest() throws Exception {
    mThrown.expect(AuthenticationException.class);
    mThrown.expectMessage("Only allow the user starting with tachyon");

    String authenticateId = "not-tachyon-1";
    NameCallback ncb = new NameCallback(" authentication id: ");
    ncb.setName(authenticateId);

    PasswordCallback pcb = new PasswordCallback(" password: ", false);
    pcb.setPassword("anonymous".toCharArray());

    Callback[] callbacks = new Callback[]{ncb, pcb,
        new AuthorizeCallback(authenticateId, authenticateId)};
    mPlainServerCBHandler.handle(callbacks);
  }

  public static class NameMatchAuthenticationProvider implements AuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!user.matches("^tachyon.*")) {
        throw new AuthenticationException("Only allow the user starting with tachyon");
      }
    }
  }
}
