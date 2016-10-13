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
import alluxio.client.AlluxioStorageType;
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.client.util.ClientTestUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link InStreamOptions} class.
 */
public class InStreamOptionsTest {
  /**
   * Tests that building an {@link InStreamOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() {
    InStreamOptions options = InStreamOptions.defaults();
    Assert.assertEquals(AlluxioStorageType.PROMOTE, options.getAlluxioStorageType());
    Assert.assertEquals(Constants.MB, options.getSeekBufferSizeBytes());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    ReadType readType = ReadType.NO_CACHE;
    FileWriteLocationPolicy policy = new RoundRobinPolicy();

    InStreamOptions options = InStreamOptions.defaults();
    options.setReadType(readType);
    options.setLocationPolicy(policy);
    options.setCachePartiallyReadBlock(true);
    options.setSeekBufferSizeBytes(Constants.MB);

    Assert.assertEquals(options.getAlluxioStorageType(), readType.getAlluxioStorageType());
    Assert.assertEquals(policy, options.getLocationPolicy());
    Assert.assertTrue(options.isCachePartiallyReadBlock());
    Assert.assertEquals(Constants.MB, options.getSeekBufferSizeBytes());
  }

  /**
   * Tests that building a {@link InStreamOptions} with a modified configuration works.
   */
  @Test
  public void modifiedConf() {
    Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.NO_CACHE.toString());
    try {
      InStreamOptions options = InStreamOptions.defaults();
      Assert.assertEquals(ReadType.NO_CACHE.getAlluxioStorageType(),
          options.getAlluxioStorageType());
    } finally {
      ClientTestUtils.resetClient();
    }
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(InStreamOptions.class);
  }
}
