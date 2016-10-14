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
import alluxio.client.WriteType;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Random;

/**
 * Tests for the {@link CreateDirectoryOptions} class.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Mode to use CommonTestUtils#testEquals.
@PrepareForTest(Mode.class)
public class CreateDirectoryOptionsTest {
  private final WriteType mDefaultWriteType =
      Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);

  @Test
  public void defaults() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    Assert.assertFalse(options.isAllowExists());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(mDefaultWriteType.getUnderStorageType(), options.getUnderStorageType());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    Mode mode = new Mode((short) 0123);
    WriteType writeType = WriteType.NONE;

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    options.setAllowExists(allowExists);
    options.setMode(mode);
    options.setRecursive(recursive);
    options.setWriteType(writeType);

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(mode, options.getMode());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    Mode mode = new Mode((short) 0123);
    WriteType writeType = WriteType.NONE;

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    options.setAllowExists(allowExists);
    options.setMode(mode);
    options.setRecursive(recursive);
    options.setWriteType(writeType);

    CreateDirectoryTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(allowExists, thriftOptions.isAllowExists());
    Assert.assertEquals(recursive, thriftOptions.isRecursive());
    Assert.assertEquals(writeType.getUnderStorageType().isSyncPersist(),
        thriftOptions.isPersisted());
    Assert.assertEquals(mode.toShort(), thriftOptions.getMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateDirectoryOptions.class);
  }
}
