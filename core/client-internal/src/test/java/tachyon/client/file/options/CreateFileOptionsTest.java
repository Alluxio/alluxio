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
import tachyon.client.WriteType;
import tachyon.client.file.policy.FileWriteLocationPolicy;
import tachyon.client.file.policy.RoundRobinPolicy;
import tachyon.thrift.CreateFileTOptions;

/**
 * Tests for the {@link CreateFileOptions} class.
 */
public class CreateFileOptionsTest {
  private final long mDefaultBlockSizeBytes = ClientContext.getConf().getBytes(
      Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
  private final WriteType mDefaultWriteType = ClientContext.getConf().getEnum(
      Constants.USER_FILE_WRITE_TYPE_DEFAULT, tachyon.client.WriteType.class);

  // TODO(calvin): Test location policy when a factory is created
  @Test
  public void defaultsTest() {
    CreateFileOptions options = CreateFileOptions.defaults();
    Assert.assertTrue(options.isRecursive());
    Assert.assertEquals(mDefaultBlockSizeBytes, options.getBlockSizeBytes());
    Assert.assertEquals(mDefaultWriteType.getTachyonStorageType(), options.getTachyonStorageType());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType(), options.getUnderStorageType());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    Random random = new Random();
    long blockSize = random.nextLong();
    FileWriteLocationPolicy policy = new RoundRobinPolicy();
    boolean recursive = random.nextBoolean();
    long ttl = random.nextLong();
    WriteType writeType = WriteType.NONE;

    CreateFileOptions options = CreateFileOptions.defaults();
    options.setBlockSizeBytes(blockSize);
    options.setLocationPolicy(policy);
    options.setRecursive(recursive);
    options.setTtl(ttl);
    options.setWriteType(writeType);

    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(policy, options.getLocationPolicy());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(writeType.getTachyonStorageType(), options.getTachyonStorageType());
    Assert.assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThriftTest() {
    CreateFileOptions options = CreateFileOptions.defaults();
    CreateFileTOptions thriftOptions = options.toThrift();
    Assert.assertTrue(thriftOptions.isRecursive());
    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType().isSyncPersist(), thriftOptions
        .isPersisted());
    Assert.assertEquals(mDefaultBlockSizeBytes, thriftOptions.getBlockSizeBytes());
    Assert.assertEquals(Constants.NO_TTL, thriftOptions.getTtl());
  }
}
