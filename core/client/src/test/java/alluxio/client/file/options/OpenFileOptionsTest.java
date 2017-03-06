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
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link OpenFileOptions} class.
 */
public class OpenFileOptionsTest {
  private final ReadType mDefaultReadType =
      Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);

  @Test
  public void defaults() {
    OpenFileOptions options = OpenFileOptions.defaults();
    Assert.assertEquals(mDefaultReadType, options.getReadType());
    Assert.assertEquals(Integer.MAX_VALUE, options.getMaxUfsReadConcurrency());
    Assert.assertTrue(options.getUfsReadLocationPolicy() instanceof LocalFirstPolicy);
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    FileWriteLocationPolicy policy = new RoundRobinPolicy();
    ReadType readType = ReadType.NO_CACHE;

    OpenFileOptions options = OpenFileOptions.defaults();
    options.setReadType(readType);
    options.setCacheLocationPolicy(policy);
    options.setMaxUfsReadConcurrency(5);
    options.setUfsReadLocationPolicy((BlockLocationPolicy) policy);

    Assert.assertEquals(readType, options.getReadType());
    Assert.assertEquals(policy, options.getCacheLocationPolicy());
    Assert.assertEquals(5, options.getMaxUfsReadConcurrency());
    Assert.assertEquals(policy, options.getUfsReadLocationPolicy());
  }

  /**
   * Tests conversion to {@link InStreamOptions}.
   */
  @Test
  public void toInStreamOptions() {
    OpenFileOptions options = OpenFileOptions.defaults();
    InStreamOptions inStreamOptions = options.toInStreamOptions();
    Assert.assertEquals(options.getReadType().getAlluxioStorageType(),
        inStreamOptions.getAlluxioStorageType());
    Assert.assertEquals(options.getCacheLocationPolicy(), inStreamOptions.getCacheLocationPolicy());
    Assert.assertEquals(options.getUfsReadLocationPolicy(),
        inStreamOptions.getUfsReadLocationPolicy());
    Assert.assertEquals(options.getMaxUfsReadConcurrency(),
        inStreamOptions.getMaxUfsReadConcurrency());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(OpenFileOptions.class);
  }
}
