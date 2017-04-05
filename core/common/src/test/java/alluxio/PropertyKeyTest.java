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

package alluxio;

import alluxio.exception.ExceptionMessage;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests enum type {@link PropertyKey}.
 */
public final class PropertyKeyTest {

  /**
   * Tests parsing string to PropertyKey by {@link PropertyKey#fromString}.
   */
  @Test
  public void fromString() throws Exception {
    Assert
        .assertEquals(PropertyKey.VERSION, PropertyKey.fromString(PropertyKey.VERSION.toString()));
  }

  @Test
  public void equalsTest() throws Exception {
    Assert.assertEquals(
        ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO.format("foo"),
        ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO.format("foo"));
    Assert.assertNotEquals(PropertyKey.HOME, PropertyKey.HOME);
  }

  @Test
  public void length() throws Exception {
    Assert.assertEquals(PropertyKey.Name.HOME.length(), PropertyKey.HOME.length());
  }

  @Test
  public void isValid() throws Exception {
    Assert.assertTrue(PropertyKey.isValid(PropertyKey.HOME.toString()));
    Assert.assertTrue(PropertyKey.isValid(
        ParameterizedPropertyKey.Template.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO.format("foo")
            .toString()));
    Assert.assertFalse(PropertyKey.isValid(""));
    Assert.assertFalse(PropertyKey.isValid(" "));
    Assert.assertFalse(PropertyKey.isValid("foo"));
    Assert.assertFalse(PropertyKey.isValid(PropertyKey.HOME.toString() + "1"));
    Assert.assertFalse(PropertyKey.isValid(PropertyKey.HOME.toString().toUpperCase()));
  }

  @Test
  public void fromStringExceptionThrown() throws Exception {
    String[] wrongKeys =
        {"", " ", "foo", "alluxio.foo", "alluxio.HOME", "alluxio.master.mount.table.root.alluxio1",
            "alluxio.master.mount.table.alluxio", "alluxio.master.mount.table.foo"};
    for (String key : wrongKeys) {
      try {
        PropertyKey.fromString(key);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(e.getMessage(),
            ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
      }
    }
  }
}
