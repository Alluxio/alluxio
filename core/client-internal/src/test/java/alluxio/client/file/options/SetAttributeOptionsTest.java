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

import alluxio.thrift.SetAttributeTOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link SetAttributeOptions} class.
 */
public class SetAttributeOptionsTest {
  @Test
  public void defaultsTest() {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    Assert.assertFalse(options.hasPersisted());
    Assert.assertFalse(options.hasPinned());
    Assert.assertFalse(options.hasTtl());
    Assert.assertFalse(options.hasOwner());
    Assert.assertFalse(options.hasGroup());
    Assert.assertFalse(options.hasPermission());
    Assert.assertFalse(options.isRecursive());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    Random random = new Random();
    boolean persisted = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    long ttl = random.nextLong();
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String owner = new String(bytes);
    random.nextBytes(bytes);
    String group = new String(bytes);
    short permission = (short) random.nextInt();
    boolean recursive = random.nextBoolean();

    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.setPersisted(persisted);
    options.setPinned(pinned);
    options.setTtl(ttl);
    options.setOwner(owner);
    options.setGroup(group);
    options.setPermission(permission);
    options.setRecursive(recursive);

    Assert.assertTrue(options.hasPersisted());
    Assert.assertEquals(persisted, options.getPersisted());
    Assert.assertTrue(options.hasPinned());
    Assert.assertEquals(pinned, options.getPinned());
    Assert.assertTrue(options.hasTtl());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertTrue(options.hasOwner());
    Assert.assertEquals(owner, options.getOwner());
    Assert.assertTrue(options.hasGroup());
    Assert.assertEquals(group, options.getGroup());
    Assert.assertTrue(options.hasPermission());
    Assert.assertEquals(permission, options.getPermission());
    Assert.assertEquals(recursive, options.isRecursive());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThriftTest() {
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
    Assert.assertEquals(ttl, thriftOptions.getTtl());
  }
}
