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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.security.authentication.AuthenticationFactory;

/**
 * Unit test for methods in {@link tachyon.security.UserInformation},
 * which gets or creates a Tachyon user
 */
public class UserInformationTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    UserInformation.reset();
  }

  /**
   * Test whether we can get login user with conf in SIMPLE mode.
   * @throws Exception
   */
  @Test
  public void getSimpleLoginUserTest() throws Exception {
    UserInformation.setsAuthType(AuthenticationFactory.AuthType.SIMPLE);

    User loginUser = UserInformation.getTachyonLoginUser();

    Assert.assertNotNull(loginUser);
    Assert.assertFalse(loginUser.getName().isEmpty());
  }

  // TODO: getKerberosLoginUserTest()

  // TODO: createRemoteUserTest()

  /**
   * Test whether we can get exception when getting a login user in non-security mode
   * @throws Exception
   */
  @Test
  public void securityEnabledTest() throws Exception {
    // TODO: add Kerberos in the white list when it is supported.
    // throw exception when AuthType is not "SIMPLE"
    UserInformation.setsAuthType(AuthenticationFactory.AuthType.NOSASL);
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("UserInformation is only supported in SIMPLE mode");
    UserInformation.getTachyonLoginUser();
  }
}
