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

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.WriteType;
import tachyon.thrift.CreateTOptions;

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
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(mDefaultBlockSizeBytes, options.getBlockSizeBytes());
    Assert.assertEquals(mDefaultWriteType.getTachyonStorageType(), options.getTachyonStorageType());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType(), options.getUnderStorageType());
    Assert.assertEquals(Constants.NO_TTL, options.getTTL());
  }

  @Test
  public void toThriftTest() {
    CreateFileOptions options = CreateFileOptions.defaults();
    CreateTOptions thriftOptions = options.toThrift();
    Assert.assertFalse(thriftOptions.isRecursive());
    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType().isSyncPersist(), thriftOptions
        .isPersisted());
    Assert.assertEquals(mDefaultBlockSizeBytes, thriftOptions.getBlockSizeBytes());
    Assert.assertEquals(Constants.NO_TTL, thriftOptions.getTtl());
  }
}
