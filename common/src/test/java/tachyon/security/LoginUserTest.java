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

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit test for {@link tachyon.security.LoginUser}.
 */
public final class LoginUserTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * User reflection to reset the private static member sLoginUser in LoginUser.
   *
   * @throws Exception thrown if the user fields cannot be set
   */
  @Before
  public void before() throws Exception {
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }

  /**
   * Test whether we can get login user with conf in SIMPLE mode.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void getSimpleLoginUserTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(loginUser.getName(), System.getProperty("user.name"));
  }

  /**
   * Test whether we can get login user with conf in SIMPLE mode, when user name is provided by
   * the application through configuration.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void getSimpleLoginUserProvidedByAppTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
    //TODO(qifan): after TachyonConf is refactored into Singleton, we will use TachyonConf
    //instead of System.getProperty for retrieving user name.
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon-user");

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(loginUser.getName(), "tachyon-user");

    // clean up
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "");
  }

  /**
   * Test whether we can get login user with conf in SIMPLE mode, when user name is set to an
   * empty string in the application configuration. In this case, login should return the OS user
   * instead of empty string.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void getSimpleLoginUserWhenNotProvidedByAppTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
    //TODO(qifan): after TachyonConf is refactored into Singleton, we will use TachyonConf
    //instead of System.getProperty for retrieving user name.
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "");

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(loginUser.getName(), System.getProperty("user.name"));
  }

  /**
   * Test whether we can get login user with conf in CUSTOM mode.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void getCustomLoginUserTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(loginUser.getName(), System.getProperty("user.name"));
  }

  /**
   * Test whether we can get login user with conf in CUSTOM mode, when user name is provided by
   * the application through configuration.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void getCustomLoginUserProvidedByAppTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");
    //TODO(dong): after TachyonConf is refactored into Singleton, we will use TachyonConf
    //instead of System.getProperty for retrieving user name.
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "tachyon-user");

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(loginUser.getName(), "tachyon-user");

    // clean up
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "");
  }

  /**
   * Test whether we can get login user with conf in CUSTOM mode, when user name is set to an
   * empty string in the application configuration. In this case, login should return the OS user
   * instead of empty string.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void getCustomLoginUserWhenNotProvidedByAppTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "CUSTOM");
    //TODO(dong): after TachyonConf is refactored into Singleton, we will use TachyonConf
    //instead of System.getProperty for retrieving user name.
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "");

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(loginUser.getName(), System.getProperty("user.name"));
  }

  // TODO(dong): getKerberosLoginUserTest()

  /**
   * Test whether we can get exception when getting a login user in non-security mode.
   *
   * @throws Exception thrown if the current user cannot be retrieved
   */
  @Test
  public void securityEnabledTest() throws Exception {
    // TODO(dong): add Kerberos in the white list when it is supported.
    // throw exception when AuthType is not "SIMPLE", or "CUSTOM"
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "NOSASL");

    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("User is not supported in NOSASL mode");
    LoginUser.get(conf);
  }
}
