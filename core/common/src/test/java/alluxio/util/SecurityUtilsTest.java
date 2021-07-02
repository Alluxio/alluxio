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

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Before;
import org.junit.Test;

public final class SecurityUtilsTest {

  private InstancedConfiguration mConfiguration;

  @Before
  public void before() {
    mConfiguration = ConfigurationTestUtils.defaults();
  }

  /**
   * Tests the {@link SecurityUtils#getOwnerFromGrpcClient()} ()} method.
   */
  @Test
  public void getOwnerFromGrpcClient() throws Exception {
    // When security is not enabled, user and group are not set
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    assertEquals("", SecurityUtils.getOwnerFromGrpcClient(mConfiguration));

    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    AuthenticatedClientUser.set("test_client_user");
    assertEquals("test_client_user", SecurityUtils.getOwnerFromGrpcClient(mConfiguration));
  }

  /**
   * Tests the {@link SecurityUtils#getGroupFromGrpcClient()} ()} method.
   */
  @Test
  public void getGroupFromGrpcClient() throws Exception {
    // When security is not enabled, user and group are not set
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    assertEquals("", SecurityUtils.getGroupFromGrpcClient(mConfiguration));

    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    AuthenticatedClientUser.set("test_client_user");
    assertEquals("test_client_user", SecurityUtils.getGroupFromGrpcClient(mConfiguration));
  }
}
