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

import alluxio.AuthenticatedUserRule;
import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.security.authorization.Permission;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for the {@link OutStreamOptions} class.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Permission to use CommonTestUtils#testEquals.
@PrepareForTest(Permission.class)
public class OutStreamOptionsTest {
  @Rule
  public AuthenticatedUserRule mRule = new AuthenticatedUserRule("test");

  /**
   * Tests that building an {@link OutStreamOptions} with the defaults works.
   */
  @Test
  public void defaults() throws IOException {
    AlluxioStorageType alluxioType = AlluxioStorageType.STORE;
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH.toString());

    OutStreamOptions options = OutStreamOptions.defaults();

    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    Assert.assertEquals(alluxioType, options.getAlluxioStorageType());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(TtlAction.DELETE, options.getTtlAction());
    Assert.assertEquals(ufsType, options.getUnderStorageType());
    Assert.assertTrue(options.getLocationPolicy() instanceof LocalFirstPolicy);
    Assert.assertEquals(Permission.defaults().applyFileUMask().setOwnerFromLoginModule(),
        options.getPermission());
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long blockSize = random.nextLong();
    FileWriteLocationPolicy policy = new RoundRobinPolicy();
    long ttl = random.nextLong();
    WriteType writeType = WriteType.NONE;
    Permission perm = Permission.defaults();

    OutStreamOptions options = OutStreamOptions.defaults();
    options.setBlockSizeBytes(blockSize);
    options.setLocationPolicy(policy);
    options.setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    options.setWriteType(writeType);
    options.setPermission(perm);

    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(policy, options.getLocationPolicy());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(TtlAction.FREE, options.getTtlAction());
    Assert.assertEquals(writeType.getAlluxioStorageType(), options.getAlluxioStorageType());
    Assert.assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
    Assert.assertEquals(perm, options.getPermission());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(OutStreamOptions.class);
  }
}
