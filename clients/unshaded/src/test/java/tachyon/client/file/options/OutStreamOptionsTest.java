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

package tachyon.client.file.options;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.WriteType;
import tachyon.client.file.policy.FileWriteLocationPolicy;
import tachyon.client.file.policy.LocalFirstPolicy;
import tachyon.client.file.policy.RoundRobinPolicy;
import tachyon.conf.TachyonConf;

/**
 * Tests for the {@link OutStreamOptions} class.
 */
public class OutStreamOptionsTest {

  /**
   * Tests that building an {@link OutStreamOptions} works.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    long blockSize = random.nextLong();
    TachyonStorageType tachyonType = TachyonStorageType.STORE;
    long ttl = random.nextLong();
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;
    FileWriteLocationPolicy policy = new RoundRobinPolicy();

    OutStreamOptions options =
        new OutStreamOptions.Builder(new TachyonConf())
            .setBlockSizeBytes(blockSize)
            .setTachyonStorageType(tachyonType)
            .setTtl(ttl)
            .setUnderStorageType(ufsType)
            .setLocationPolicy(policy)
            .build();

    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(tachyonType, options.getTachyonStorageType());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(ufsType, options.getUnderStorageType());
    Assert.assertEquals(policy, options.getLocationPolicy());
  }

  /**
   * Tests that building an {@link OutStreamOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() {
    TachyonStorageType tachyonType = TachyonStorageType.STORE;
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    conf.set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH.toString());
    ClientContext.reset(conf);

    OutStreamOptions options = OutStreamOptions.defaults();

    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    Assert.assertEquals(tachyonType, options.getTachyonStorageType());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(ufsType, options.getUnderStorageType());
    Assert.assertTrue(options.getLocationPolicy() instanceof LocalFirstPolicy);
    ClientContext.reset();
  }
}
