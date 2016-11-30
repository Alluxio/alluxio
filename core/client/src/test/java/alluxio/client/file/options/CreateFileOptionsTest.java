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

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateFileTOptions;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Random;

/**
 * Tests for the {@link CreateFileOptions} class.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Mode to use CommonTestUtils#testEquals.
@PrepareForTest(Mode.class)
public class CreateFileOptionsTest {
  private final long mDefaultBlockSizeBytes = Configuration.getBytes(
      PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  private final WriteType mDefaultWriteType = Configuration.getEnum(
      PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, alluxio.client.WriteType.class);

  // TODO(calvin): Test location policy when a factory is created
  @Test
  public void defaults() {
    CreateFileOptions options = CreateFileOptions.defaults();
    Assert.assertTrue(options.isRecursive());
    Assert.assertEquals(mDefaultBlockSizeBytes, options.getBlockSizeBytes());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(TtlAction.DELETE, options.getTtlAction());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long blockSize = random.nextLong();
    FileWriteLocationPolicy policy = new RoundRobinPolicy();
    Mode mode = new Mode((short) 0123);
    boolean recursive = random.nextBoolean();
    long ttl = random.nextLong();
    WriteType writeType = WriteType.NONE;

    CreateFileOptions options = CreateFileOptions.defaults();
    options.setBlockSizeBytes(blockSize);
    options.setLocationPolicy(policy);
    options.setMode(mode);
    options.setRecursive(recursive);
    options.setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    options.setWriteType(writeType);

    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(policy, options.getLocationPolicy());
    Assert.assertEquals(mode, options.getMode());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(TtlAction.FREE, options.getTtlAction());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    CreateFileOptions options = CreateFileOptions.defaults();
    CreateFileTOptions thriftOptions = options.toThrift();
    Assert.assertTrue(thriftOptions.isRecursive());
    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType().isSyncPersist(), thriftOptions
        .isPersisted());
    Assert.assertEquals(mDefaultBlockSizeBytes, thriftOptions.getBlockSizeBytes());
    Assert.assertEquals(Constants.NO_TTL, thriftOptions.getTtl());
    Assert.assertEquals(alluxio.thrift.TTtlAction.Delete, thriftOptions.getTtlAction());
    Assert.assertFalse(thriftOptions.isSetMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateFileOptions.class);
  }
}
