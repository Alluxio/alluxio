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

package alluxio.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit test for {@link alluxio.security.LoginUser}.
 */
public final class LoginUserTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  @Before
  public void before() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }

  @After
  public void after() {
    mConfiguration = ConfigurationTestUtils.defaults();
  }

  /**
   * Tests whether we can get login user with conf in SIMPLE mode.
   */
  @Test
  public void getSimpleLoginUser() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());

    User loginUser = LoginUser.get(mConfiguration);

    assertNotNull(loginUser);
    assertEquals(System.getProperty("user.name"), loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in SIMPLE mode, when user name is provided by
   * the application through configuration.
   */
  @Test
  public void getSimpleLoginUserProvidedByApp() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "alluxio-user");

    User loginUser = LoginUser.get(mConfiguration);

    assertNotNull(loginUser);
    assertEquals("alluxio-user", loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in SIMPLE mode, when a user list is provided by
   * by the application through configuration.
   */
  @Test
  public void getSimpleLoginUserListProvidedByApp() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "alluxio-user, superuser");

    User loginUser = LoginUser.get(mConfiguration);

    // The user list is considered as a single user name.
    assertNotNull(loginUser);
    assertEquals("alluxio-user, superuser", loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in SIMPLE mode, when user name is set to an
   * empty string in the application configuration. In this case, login should return the OS user
   * instead of empty string.
   */
  @Test
  public void getSimpleLoginUserWhenNotProvidedByApp() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mConfiguration.unset(PropertyKey.SECURITY_LOGIN_USERNAME);

    User loginUser = LoginUser.get(mConfiguration);

    assertNotNull(loginUser);
    assertEquals(System.getProperty("user.name"), loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in CUSTOM mode.
   */
  @Test
  public void getCustomLoginUser() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());

    User loginUser = LoginUser.get(mConfiguration);

    assertNotNull(loginUser);
    assertEquals(System.getProperty("user.name"), loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in CUSTOM mode, when user name is provided by
   * the application through configuration.
   */
  @Test
  public void getCustomLoginUserProvidedByApp() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "alluxio-user");

    User loginUser = LoginUser.get(mConfiguration);

    assertNotNull(loginUser);
    assertEquals("alluxio-user", loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in CUSTOM mode, when user name is set to an
   * empty string in the application configuration. In this case, login should return the OS user
   * instead of empty string.
   */
  @Test
  public void getCustomLoginUserWhenNotProvidedByApp() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());
    mConfiguration.unset(PropertyKey.SECURITY_LOGIN_USERNAME);

    User loginUser = LoginUser.get(mConfiguration);

    assertNotNull(loginUser);
    assertEquals(System.getProperty("user.name"), loginUser.getName());
  }

  // TODO(dong): getKerberosLoginUserTest()

  /**
   * Tests whether we can get exception when getting a login user in non-security mode.
   */
  @Test
  public void securityEnabled() throws Exception {
    // TODO(dong): add Kerberos in the white list when it is supported.
    // throw exception when AuthType is not "SIMPLE", or "CUSTOM"
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());

    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("User is not supported in NOSASL mode");
    LoginUser.get(mConfiguration);
  }
}
