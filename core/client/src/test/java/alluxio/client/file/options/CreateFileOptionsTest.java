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
import alluxio.client.ClientContext;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.thrift.CreateFileTOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link CreateFileOptions} class.
 */
public class CreateFileOptionsTest {
  private final long mDefaultBlockSizeBytes = ClientContext.getConf().getBytes(
      Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
  private final WriteType mDefaultWriteType = ClientContext.getConf().getEnum(
      Constants.USER_FILE_WRITE_TYPE_DEFAULT, alluxio.client.WriteType.class);

  // TODO(calvin): Test location policy when a factory is created
  @Test
  public void defaultsTest() {
    CreateFileOptions options = CreateFileOptions.defaults();
    Assert.assertTrue(options.isRecursive());
    Assert.assertEquals(mDefaultBlockSizeBytes, options.getBlockSizeBytes());
    Assert.assertEquals(mDefaultWriteType.getAlluxioStorageType(), options.getAlluxioStorageType());
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
    Assert.assertEquals(writeType.getAlluxioStorageType(), options.getAlluxioStorageType());
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
