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

package alluxio.master.file.options;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.security.authorization.Permission;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Random;

/**
 * Unit tests for {@link CreateDirectoryOptions}.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Permission to use CommonTestUtils#testEquals.
@PrepareForTest(Permission.class)
public class CreateDirectoryOptionsTest {
  /**
   * Tests the {@link CreateDirectoryOptions#defaults()} method.
   */
  @Test
  public void defaults() throws Exception {
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();

    Assert.assertEquals(false, options.isAllowExists());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean mountPoint = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    Permission permission = Permission.defaults();
    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults()
        .setAllowExists(allowExists)
        .setMountPoint(mountPoint)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setPermission(permission)
        .setRecursive(recursive);

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(mountPoint, options.isMountPoint());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(permission, options.getPermission());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(recursive, options.isRecursive());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateDirectoryOptions.class, "mOperationTimeMs");
  }
}
