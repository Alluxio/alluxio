/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.file.options;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.client.util.ClientTestUtils;

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
