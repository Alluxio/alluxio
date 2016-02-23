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
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link OpenFileOptions} class.
 */
public class OpenFileOptionsTest {
  private final ReadType mDefaultReadType =
      ClientContext.getConf().getEnum(Constants.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);

  @Test
  public void defaultsTest() {
    OpenFileOptions options = OpenFileOptions.defaults();
    Assert.assertEquals(mDefaultReadType.getAlluxioStorageType(), options.getAlluxioStorageType());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    FileWriteLocationPolicy policy = new RoundRobinPolicy();
    ReadType readType = ReadType.NO_CACHE;

    OpenFileOptions options = OpenFileOptions.defaults();
    options.setReadType(readType);
    options.setLocationPolicy(policy);

    Assert.assertEquals(readType.getAlluxioStorageType(), options.getAlluxioStorageType());
    Assert.assertEquals(policy, options.getLocationPolicy());
  }

  /**
   * Tests conversion to {@link InStreamOptions}.
   */
  @Test
  public void toInStreamOptionsTest() {
    OpenFileOptions options = OpenFileOptions.defaults();
    InStreamOptions inStreamOptions = options.toInStreamOptions();
    Assert.assertEquals(options.getAlluxioStorageType(),
        inStreamOptions.getAlluxioStorageType());
    Assert.assertEquals(options.getLocationPolicy(), inStreamOptions.getLocationPolicy());
  }
}
