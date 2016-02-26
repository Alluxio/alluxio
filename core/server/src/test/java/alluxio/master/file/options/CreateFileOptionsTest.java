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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link CreateFileOptions}.
 */
public class CreateFileOptionsTest {
  /**
   * Tests the {@link CreateFileOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    MasterContext.reset(conf);

    CreateFileOptions options = CreateFileOptions.defaults();

    Assert.assertEquals(false, options.isAllowExists());
    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    Assert.assertFalse(options.isDirectory());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    MasterContext.reset();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() throws Exception {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    long blockSize = random.nextLong();
    boolean mountPoint = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    PermissionStatus permissionStatus = PermissionStatus.getDirDefault();
    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    long ttl = random.nextLong();

    CreateFileOptions options = CreateFileOptions.defaults()
        .setAllowExists(allowExists)
        .setBlockSizeBytes(blockSize)
        .setMountPoint(mountPoint)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setPermissionStatus(permissionStatus)
        .setRecursive(recursive)
        .setTtl(ttl);

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(mountPoint, options.isMountPoint());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(permissionStatus, options.getPermissionStatus());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ttl, options.getTtl());
  }
}
