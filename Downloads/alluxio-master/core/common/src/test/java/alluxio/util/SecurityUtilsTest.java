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

package alluxio.util;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public final class SecurityUtilsTest {

  @After
  public void after() {
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests the {@link SecurityUtils#getOwnerFromThriftClient()} ()} method.
   */
  @Test
  public void getOwnerFromThriftClient() throws Exception {
    // When security is not enabled, user and group are not set
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    Assert.assertEquals("", SecurityUtils.getOwnerFromThriftClient());

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    AuthenticatedClientUser.set("test_client_user");
    Assert.assertEquals("test_client_user", SecurityUtils.getOwnerFromThriftClient());
  }

  /**
   * Tests the {@link SecurityUtils#getGroupFromThriftClient()} ()} method.
   */
  @Test
  public void getGroupFromThriftClient() throws Exception {
    // When security is not enabled, user and group are not set
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    Assert.assertEquals("", SecurityUtils.getGroupFromThriftClient());

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    AuthenticatedClientUser.set("test_client_user");
    Assert.assertEquals("test_client_user", SecurityUtils.getGroupFromThriftClient());
  }

  /**
   * Tests the {@link SecurityUtils#getOwnerFromLoginModule()} method.
   */
  @Test
  public void getOwnerFromLoginModule() throws Exception {
    // When security is not enabled, user and group are not set
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    Assert.assertEquals("", SecurityUtils.getOwnerFromLoginModule());

    // When authentication is enabled, user and group are inferred from login module
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "test_login_user");
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    Assert.assertEquals("test_login_user", SecurityUtils.getOwnerFromLoginModule());
  }

  /**
   * Tests the {@link SecurityUtils#getGroupFromLoginModule()} method.
   */
  @Test
  public void getGroupFromLoginModuleError() throws Exception {
    // When security is not enabled, user and group are not set
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    Assert.assertEquals("", SecurityUtils.getGroupFromLoginModule());

    // When authentication is enabled, user and group are inferred from login module
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "test_login_user");
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    LoginUserTestUtils.resetLoginUser();
    Assert.assertEquals("test_login_user", SecurityUtils.getGroupFromLoginModule());
  }
}
