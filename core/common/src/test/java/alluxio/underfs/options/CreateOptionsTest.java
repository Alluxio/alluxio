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

package alluxio.underfs.options;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Permission;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Tests for the {@link CreateOptions} class.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Permission to use CommonTestUtils#testEquals.
@PrepareForTest(Permission.class)
public final class CreateOptionsTest {
  /**
   * Tests for default {@link CreateOptions}.
   */
  @Test
  public void defaultsTest() throws IOException {
    CreateOptions options = new CreateOptions();

    Permission expectedPs = Permission.defaults().applyFileUMask();

    // Verify that the owner and group are not set.
    Assert.assertEquals("", options.getPermission().getOwner());
    Assert.assertEquals("", options.getPermission().getGroup());
    Assert.assertEquals(expectedPs.getMode().toShort(),
        options.getPermission().getMode().toShort());
  }

  /**
   * Tests for building an {@link CreateOptions} with a security enabled
   * configuration.
   */
  @Test
  public void securityEnabledTest() throws IOException {
    Configuration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(Constants.SECURITY_LOGIN_USERNAME, "foo");
    // Use IdentityUserGroupMapping to map user "foo" to group "foo".
    Configuration.set(Constants.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());

    CreateOptions options = new CreateOptions();

    Permission expectedPs = Permission.defaults().applyFileUMask();

    // Verify that the owner and group are not set.
    Assert.assertEquals("", options.getPermission().getOwner());
    Assert.assertEquals("", options.getPermission().getGroup());
    Assert.assertEquals(expectedPs.getMode().toShort(),
        options.getPermission().getMode().toShort());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    Permission perm = Permission.defaults();

    CreateOptions options = new CreateOptions();
    options.setPermission(perm);

    Assert.assertEquals(perm, options.getPermission());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateOptions.class);
  }
}
