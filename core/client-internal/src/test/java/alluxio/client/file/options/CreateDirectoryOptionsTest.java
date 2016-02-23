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
import alluxio.thrift.CreateDirectoryTOptions;

import org.junit.Assert;
import org.junit.Test;

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
   * Tests getting and setting fields.
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

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThriftTest() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    WriteType writeType = WriteType.NONE;

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    options.setAllowExists(allowExists);
    options.setRecursive(recursive);
    options.setWriteType(writeType);

    CreateDirectoryTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(allowExists, thriftOptions.isAllowExists());
    Assert.assertEquals(recursive, thriftOptions.isRecursive());
    Assert.assertEquals(writeType.getUnderStorageType().isSyncPersist(),
        thriftOptions.isPersisted());
  }
}
