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
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit test for inner class {@link tachyon.security.authentication.AuthenticationFactory
 * .AuthTypes} and methods of {@link tachyon.security.authentication.AuthenticationFactory}
 */
public class AuthenticationFactoryTest {

  @Test
  public void authTypesTest() {
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    AuthenticationFactory authenticationFactory = new AuthenticationFactory(tachyonConf);

    Assert.assertEquals(AuthenticationFactory.AuthTypes.SIMPLE.getAuthName(),
        authenticationFactory.getAuthTypeStr());
  }

  // TODO: add more tests for methods of AuthenticationFactory
}
