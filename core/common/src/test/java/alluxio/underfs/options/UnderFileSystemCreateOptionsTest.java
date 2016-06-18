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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.authorization.PermissionStatus;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for the {@link UnderFileSystemCreateOptions} class.
 */
public class UnderFileSystemCreateOptionsTest {
  /**
   * Tests for default {@link UnderFileSystemCreateOptions}.
   */
  @Test
  public void defaultsTest() throws IOException {
    UnderFileSystemCreateOptions options = UnderFileSystemCreateOptions.defaults();

    PermissionStatus expectedPs = PermissionStatus.defaults();
    // Verify the default block size is 64MB for HDFS.
    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeByte());
    // Verify that the owner and group are not.
    Assert.assertEquals("", options.getPermissionStatus().getUserName());
    Assert.assertEquals("", options.getPermissionStatus().getGroupName());
    Assert.assertEquals(expectedPs.getPermission().toShort(),
        options.getPermissionStatus().getPermission().toShort());
  }

  /**
   * Tests for building an {@link UnderFileSystemCreateOptions} with a security enabled
   * configuration.
   */
  @Test
  public void securityEnabledTest() throws IOException {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "foo");
    // Use IdentityUserGroupMapping to map user "foo" to group "foo".
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());

    UnderFileSystemCreateOptions options = new UnderFileSystemCreateOptions(conf);

    PermissionStatus expectedPs = PermissionStatus.defaults().applyFileUMask(conf);

    // Verify the default block size is 64MB for HDFS.
    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeByte());
    // Verify that the owner and group are not.
    Assert.assertEquals("", options.getPermissionStatus().getUserName());
    Assert.assertEquals("", options.getPermissionStatus().getGroupName());
    Assert.assertEquals(expectedPs.getPermission().toShort(),
        options.getPermissionStatus().getPermission().toShort());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    Random random = new Random();
    long blockSize = random.nextLong();
    PermissionStatus ps = PermissionStatus.defaults();

    Configuration conf = new Configuration();
    UnderFileSystemCreateOptions options = new UnderFileSystemCreateOptions(conf);
    options.setBlockSizeByte(blockSize);
    options.setPermissionStatus(ps);

    Assert.assertEquals(blockSize, options.getBlockSizeByte());
    Assert.assertEquals(ps, options.getPermissionStatus());
  }
}
