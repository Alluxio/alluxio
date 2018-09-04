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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.security.authorization.Mode;
import alluxio.test.util.CommonUtils;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.util.ModeUtils;
import alluxio.wire.TtlAction;

import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link CreateDirectoryOptions} class.
 */
public final class CreateDirectoryOptionsTest {
  private final WriteType mDefaultWriteType =
      Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);

  @Test
  public void defaults() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    assertFalse(options.isAllowExists());
    assertFalse(options.isRecursive());
    assertEquals(Constants.NO_TTL, options.getTtl());
    assertEquals(TtlAction.DELETE, options.getTtlAction());
    assertEquals(mDefaultWriteType, options.getWriteType());
    assertEquals(ModeUtils.applyDirectoryUMask(Mode.defaults()), options.getMode());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    Mode mode = new Mode((short) random.nextInt());
    long ttl = random.nextLong();
    WriteType writeType = WriteType.NONE;

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    options.setAllowExists(allowExists);
    options.setMode(mode);
    options.setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    options.setRecursive(recursive);
    options.setWriteType(writeType);

    assertEquals(allowExists, options.isAllowExists());
    assertEquals(mode, options.getMode());
    assertEquals(recursive, options.isRecursive());
    assertEquals(ttl, options.getTtl());
    assertEquals(TtlAction.FREE, options.getTtlAction());
    assertEquals(writeType, options.getWriteType());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    Mode mode = new Mode((short) random.nextInt());
    long ttl = random.nextLong();
    WriteType writeType = WriteType.NONE;

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
    options.setAllowExists(allowExists);
    options.setMode(mode);
    options.setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    options.setRecursive(recursive);
    options.setWriteType(writeType);

    CreateDirectoryTOptions thriftOptions = options.toThrift();
    assertEquals(allowExists, thriftOptions.isAllowExists());
    assertEquals(recursive, thriftOptions.isRecursive());
    assertEquals(writeType.isThrough(), thriftOptions.isPersisted());
    assertEquals(mode.toShort(), thriftOptions.getMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(CreateDirectoryOptions.class);
  }
}
