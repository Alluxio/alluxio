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

package alluxio.security.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link UserState}.
 */
public final class UserStateTest {
  private InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

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

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

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

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

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

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

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

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

    assertNotNull(loginUser);
    assertEquals(System.getProperty("user.name"), loginUser.getName());
  }

  /**
   * Tests whether we can get login user with conf in CUSTOM mode.
   */
  @Test
  public void getCustomLoginUser() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.CUSTOM.getAuthName());

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

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

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

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

    UserState s = UserState.Factory.create(mConfiguration);
    User loginUser = s.getUser();

    assertNotNull(loginUser);
    assertEquals(System.getProperty("user.name"), loginUser.getName());
  }

  // TODO(dong): getKerberosLoginUserTest()

  @Test
  public void securityEnabled() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());

    // without security, the user will be blank.
    User u = UserState.Factory.create(mConfiguration).getUser();
    Assert.assertEquals("", u.getName());
  }
}
