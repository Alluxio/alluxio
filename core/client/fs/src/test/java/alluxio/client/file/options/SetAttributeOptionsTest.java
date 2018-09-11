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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.security.authorization.Mode;
import alluxio.test.util.CommonUtils;
import alluxio.grpc.SetAttributePOptions;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.TtlAction;

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
    assertNull(options.getPersisted());
    assertNull(options.getPinned());
    assertNull(options.getTtl());
    assertEquals(TtlAction.DELETE, options.getTtlAction());
    assertNull(options.getOwner());
    assertNull(options.getGroup());
    assertNull(options.getMode());
    assertFalse(options.isRecursive());
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

    assertEquals(persisted, options.getPersisted());
    assertEquals(pinned, options.getPinned());
    assertEquals(ttl, options.getTtl().longValue());
    assertEquals(TtlAction.FREE, options.getTtlAction());
    assertEquals(owner, options.getOwner());
    assertEquals(group, options.getGroup());
    assertTrue(mode.toShort() == options.getMode());
    assertEquals(recursive, options.isRecursive());
  }

  /**
   * Tests conversion to proto representation.
   */
  @Test
  public void toProto() {
    Random random = new Random();
    boolean persisted = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    long ttl = random.nextLong();

    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.setPersisted(persisted);
    options.setPinned(pinned);
    options.setTtl(ttl);
    SetAttributePOptions protoOptions = GrpcUtils.toProto(options);

    assertTrue(protoOptions.hasPersisted());
    assertEquals(persisted, protoOptions.getPersisted());
    assertTrue(protoOptions.hasPinned());
    assertEquals(pinned, protoOptions.getPinned());
    assertTrue(protoOptions.hasTtl());
    assertEquals(alluxio.grpc.TtlAction.DELETE, protoOptions.getTtlAction());
    assertEquals(ttl, protoOptions.getTtl());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(SetAttributeOptions.class);
  }

  @Test
  public void setOwnerToEmptyShouldFail() throws Exception {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    try {
      options.setOwner("");
      fail("Expected setOwner to fail with empty owner field");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void setGroupToEmptyShouldFail() throws Exception {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    try {
      options.setGroup("");
      fail("Expected setGroup to fail with empty group field");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
