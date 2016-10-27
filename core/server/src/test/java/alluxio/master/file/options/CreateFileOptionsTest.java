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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authorization.Permission;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Random;

/**
 * Unit tests for {@link CreateFileOptions}.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Permission to use CommonTestUtils#testEquals.
@PrepareForTest(Permission.class)
public class CreateFileOptionsTest {

  /**
   * Tests the {@link CreateFileOptions#defaults()} method.
   */
  @Test
  public void defaults() throws Exception {
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");

    CreateFileOptions options = CreateFileOptions.defaults();

    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(TtlAction.DELETE, options.getTtlAction());
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    long blockSize = random.nextLong();
    boolean mountPoint = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    Permission permission = Permission.defaults();
    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    long ttl = random.nextLong();

    CreateFileOptions options = CreateFileOptions.defaults()
        .setBlockSizeBytes(blockSize)
        .setMountPoint(mountPoint)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setPermission(permission)
        .setRecursive(recursive)
        .setTtl(ttl)
        .setTtlAction(TtlAction.FREE);

    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(mountPoint, options.isMountPoint());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(permission, options.getPermission());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(TtlAction.FREE, options.getTtlAction());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateFileOptions.class, "mOperationTimeMs");
  }
}
