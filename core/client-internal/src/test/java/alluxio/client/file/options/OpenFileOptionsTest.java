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

import org.junit.Assert;
import org.junit.Test;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;

/**
 * Tests for the {@link OpenFileOptions} class.
 */
public class OpenFileOptionsTest {
  private final ReadType mDefaultReadType =
      ClientContext.getConf().getEnum(Constants.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);

  @Test
  public void defaultsTest() {
    OpenFileOptions options = OpenFileOptions.defaults();
    Assert.assertEquals(mDefaultReadType.getTachyonStorageType(), options.getTachyonStorageType());
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

    Assert.assertEquals(readType.getTachyonStorageType(), options.getTachyonStorageType());
    Assert.assertEquals(policy, options.getLocationPolicy());
  }

  /**
   * Tests conversion to {@link InStreamOptions}.
   */
  @Test
  public void toInStreamOptionsTest() {
    OpenFileOptions options = OpenFileOptions.defaults();
    InStreamOptions inStreamOptions = options.toInStreamOptions();
    Assert.assertEquals(options.getTachyonStorageType(),
        inStreamOptions.getTachyonStorageType());
    Assert.assertEquals(options.getLocationPolicy(), inStreamOptions.getLocationPolicy());
  }
}
