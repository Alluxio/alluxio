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

package alluxio.client.file.options;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Permission;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.thrift.CreateUfsFileTOptions;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Tests for the {@link CreateUfsFileOptions} class.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Permission to use CommonTestUtils#testEquals.
@PrepareForTest(Permission.class)
public final class CreateUfsFileOptionsTest {
  /**
   * Tests that building an {@link CreateUfsFileOptions} with the defaults works.
   */
  @Test
  public void defaults() throws IOException {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "foo");
    // Use IdentityOwnerGroupMapping to map owner "foo" to group "foo".
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());

    CreateUfsFileOptions options = CreateUfsFileOptions.defaults();

    Permission expectedPs = Permission.defaults().applyFileUMask();

    Assert.assertEquals("foo", options.getPermission().getOwner());
    Assert.assertEquals("foo", options.getPermission().getGroup());
    Assert.assertEquals(expectedPs.getMode(), options.getPermission().getMode());
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws IOException {
    CreateUfsFileOptions options = CreateUfsFileOptions.defaults();
    String owner = "test-owner";
    String group = "test-group";
    short mode = Constants.DEFAULT_FILE_SYSTEM_MODE;
    options.setPermission(new Permission(owner, group, mode));

    Assert.assertEquals(owner, options.getPermission().getOwner());
    Assert.assertEquals(group, options.getPermission().getGroup());
    Assert.assertEquals(mode, options.getPermission().getMode().toShort());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() throws IOException {
    CreateUfsFileOptions options = CreateUfsFileOptions.defaults();
    String owner = "test-owner";
    String group = "test-group";
    short mode = Constants.DEFAULT_FILE_SYSTEM_MODE;

    options.setPermission(new Permission(owner, group, mode));

    CreateUfsFileTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(owner, thriftOptions.getOwner());
    Assert.assertEquals(group, thriftOptions.getGroup());
    Assert.assertEquals(mode, thriftOptions.getMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateUfsFileOptions.class);
  }
}

