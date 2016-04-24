/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;

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
// Need to mock PermissionStatus to use CommonTestUtils#testEquals.
@PrepareForTest(PermissionStatus.class)
public class CreateDirectoryOptionsTest {
  /**
   * Tests the {@link CreateDirectoryOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    MasterContext.reset(conf);

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();

    Assert.assertEquals(false, options.isAllowExists());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
    MasterContext.reset();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() throws Exception {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean mountPoint = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    PermissionStatus permissionStatus = PermissionStatus.defaults();
    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults()
        .setAllowExists(allowExists)
        .setMountPoint(mountPoint)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setPermissionStatus(permissionStatus)
        .setRecursive(recursive);

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(mountPoint, options.isMountPoint());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(permissionStatus, options.getPermissionStatus());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(recursive, options.isRecursive());
  }

  @Test
  public void testEquals() throws Exception {
    CommonTestUtils.testEquals(CreateDirectoryOptions.class, "mOperationTimeMs");
  }
}
