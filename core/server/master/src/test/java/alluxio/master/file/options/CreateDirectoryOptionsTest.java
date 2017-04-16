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

package alluxio.master.file.options;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.Constants;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link CreateDirectoryOptions}.
 */
public class CreateDirectoryOptionsTest {
  /**
   * Tests the {@link CreateDirectoryOptions#defaults()} method.
   */
  @Test
  public void defaults() throws Exception {
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();

    Assert.assertEquals(false, options.isAllowExists());
    Assert.assertEquals("", options.getOwner());
    Assert.assertEquals("", options.getGroup());
    Assert.assertEquals(Mode.defaults().applyDirectoryUMask(), options.getMode());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    Assert.assertEquals(TtlAction.DELETE, options.getTtlAction());
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean mountPoint = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());

    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    long ttl = random.nextLong();

    CreateDirectoryOptions options = CreateDirectoryOptions.defaults()
        .setAllowExists(allowExists)
        .setMountPoint(mountPoint)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setOwner(owner)
        .setGroup(group)
        .setMode(mode)
        .setRecursive(recursive)
        .setTtl(ttl)
        .setTtlAction(TtlAction.FREE);

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(mountPoint, options.isMountPoint());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(owner, options.getOwner());
    Assert.assertEquals(group, options.getGroup());
    Assert.assertEquals(mode, options.getMode());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(TtlAction.FREE, options.getTtlAction());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateDirectoryOptions.class);
  }
}
