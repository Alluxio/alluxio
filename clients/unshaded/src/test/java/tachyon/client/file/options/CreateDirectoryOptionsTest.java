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
import tachyon.thrift.MkdirTOptions;

import java.util.Random;

/**
 * Tests for the {@link CreateDirectoryOptions} class.
 */
public class CreateDirectoryOptionsTest {
  private final WriteType mDefaultWriteType =
      ClientContext.getConf().getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);

  @Test
  public void defaultsTest() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    Assert.assertFalse(options.isAllowExists());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType(), options.getUnderStorageType());
  }

  /**
   * Tests getting and setting fields
   */
  @Test
  public void fieldsTest() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    WriteType writeType = WriteType.NONE;

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    options.setAllowExists(allowExists);
    options.setRecursive(recursive);
    options.setWriteType(writeType);

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
  }

  @Test
  public void toThriftTest() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    MkdirTOptions thriftOptions = options.toThrift();
    Assert.assertFalse(thriftOptions.isAllowExists());
    Assert.assertFalse(thriftOptions.isRecursive());
    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType().isSyncPersist(),
        thriftOptions.isPersisted());
  }
}
