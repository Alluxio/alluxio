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

package alluxio.client.file.options;

import alluxio.Constants;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ClientContext;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.client.util.ClientTestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link OutStreamOptions} class.
 */
public class OutStreamOptionsTest {
  /**
   * Tests that building an {@link OutStreamOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() {
    AlluxioStorageType alluxioType = AlluxioStorageType.STORE;
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;
    ClientContext.getConf().set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    ClientContext.getConf().set(Constants.USER_FILE_WRITE_TYPE_DEFAULT,
        WriteType.CACHE_THROUGH.toString());

    OutStreamOptions options = OutStreamOptions.defaults();

    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    Assert.assertEquals(alluxioType, options.getAlluxioStorageType());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(ufsType, options.getUnderStorageType());
    Assert.assertTrue(options.getLocationPolicy() instanceof LocalFirstPolicy);
    ClientTestUtils.resetClientContext();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    Random random = new Random();
    long blockSize = random.nextLong();
    FileWriteLocationPolicy policy = new RoundRobinPolicy();
    long ttl = random.nextLong();
    WriteType writeType = WriteType.NONE;

    OutStreamOptions options = OutStreamOptions.defaults();
    options.setBlockSizeBytes(blockSize);
    options.setLocationPolicy(policy);
    options.setTtl(ttl);
    options.setWriteType(writeType);

    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(policy, options.getLocationPolicy());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(writeType.getAlluxioStorageType(), options.getAlluxioStorageType());
    Assert.assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
  }
}
