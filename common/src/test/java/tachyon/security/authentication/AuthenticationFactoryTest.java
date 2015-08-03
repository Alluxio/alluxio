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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit test for inner class {@link tachyon.security.authentication.AuthenticationFactory
 * .AuthType} and methods of {@link tachyon.security.authentication.AuthenticationFactory}
 */
public class AuthenticationFactoryTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void authenticationFactoryConstructorTest() {
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    AuthenticationFactory authenticationFactory = new AuthenticationFactory(tachyonConf);

    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE.getAuthName(),
        authenticationFactory.getAuthTypeStr());
  }

  @Test
  public void getAuthTypeFromConfTest() {
    TachyonConf tachyonConf = new TachyonConf();
    AuthenticationFactory.AuthType authType;

    // should return a SIMPLE AuthType with conf "SIMPLE"
    tachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");
    authType = AuthenticationFactory.getAuthTypeFromConf(tachyonConf);
    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authType);

    // should return a SIMPLE AuthType with conf "simple"
    tachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "simple");
    authType = AuthenticationFactory.getAuthTypeFromConf(tachyonConf);
    Assert.assertEquals(AuthenticationFactory.AuthType.SIMPLE, authType);

    // should throw exception with conf "wrong"
    tachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "wrong");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("wrong is not a valid authentication type. Check the configuration "
        + "parameter " + Constants.TACHYON_SECURITY_AUTHENTICATION);
    authType = AuthenticationFactory.getAuthTypeFromConf(tachyonConf);
  }

  // TODO: add more tests for methods of AuthenticationFactory
}
