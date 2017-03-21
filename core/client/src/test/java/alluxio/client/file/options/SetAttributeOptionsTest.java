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
import alluxio.security.authorization.Mode;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Random;

/**
 * Tests for the {@link SetAttributeOptions} class.
 */
@RunWith(PowerMockRunner.class)
// Need to mock Mode to use CommonTestUtils#testEquals.
@PrepareForTest(Mode.class)
public final class SetAttributeOptionsTest {
  @Test
  public void defaults() {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    Assert.assertNull(options.getPersisted());
    Assert.assertNull(options.getPinned());
    Assert.assertNull(options.getTtl());
    Assert.assertEquals(TtlAction.DELETE, options.getTtlAction());
    Assert.assertNull(options.getOwner());
    Assert.assertNull(options.getGroup());
    Assert.assertNull(options.getMode());
    Assert.assertFalse(options.isRecursive());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean persisted = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    long ttl = random.nextLong();
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String owner = new String(bytes);
    random.nextBytes(bytes);
    String group = new String(bytes);
    Mode mode = new Mode((short) random.nextInt());
    boolean recursive = random.nextBoolean();

    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.setPersisted(persisted);
    options.setPinned(pinned);
    options.setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);
    options.setRecursive(recursive);

    Assert.assertEquals(persisted, options.getPersisted());
    Assert.assertEquals(pinned, options.getPinned());
    Assert.assertEquals(ttl, options.getTtl().longValue());
    Assert.assertEquals(TtlAction.FREE, options.getTtlAction());
    Assert.assertEquals(owner, options.getOwner());
    Assert.assertEquals(group, options.getGroup());
    Assert.assertEquals(mode, options.getMode());
    Assert.assertEquals(recursive, options.isRecursive());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    Random random = new Random();
    boolean persisted = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    long ttl = random.nextLong();

    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.setPersisted(persisted);
    options.setPinned(pinned);
    options.setTtl(ttl);
    SetAttributeTOptions thriftOptions = options.toThrift();

    Assert.assertTrue(thriftOptions.isSetPersisted());
    Assert.assertEquals(persisted, thriftOptions.isPersisted());
    Assert.assertTrue(thriftOptions.isSetPinned());
    Assert.assertEquals(pinned, thriftOptions.isPinned());
    Assert.assertTrue(thriftOptions.isSetTtl());
    Assert.assertEquals(alluxio.thrift.TTtlAction.Delete, thriftOptions.getTtlAction());
    Assert.assertEquals(ttl, thriftOptions.getTtl());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(SetAttributeOptions.class);
  }

  @Test
  public void setOwnerToEmptyShouldFail() throws Exception {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    try {
      options.setOwner("");
      Assert.fail("Expected setOwner to fail with empty owner field");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void setGroupToEmptyShouldFail() throws Exception {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    try {
      options.setGroup("");
      Assert.fail("Expected setGroup to fail with empty group field");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
