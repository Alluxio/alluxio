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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.exception.ExceptionMessage;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests enum type {@link PropertyKey}.
 */
public final class PropertyKeyTest {

  private PropertyKey mTestProperty = PropertyKey.create("alluxio.test.property", false,
       new String[] {"alluxio.test.property.alias1", "alluxio.test.property.alias2"});

  /**
   * Tests parsing string to PropertyKey by {@link PropertyKey#fromString}.
   */
  @Test
  public void fromString() throws Exception {
    assertEquals(PropertyKey.VERSION, PropertyKey.fromString(PropertyKey.VERSION.toString()));
    PropertyKey.fromString(PropertyKey.VERSION.toString());
    assertEquals(mTestProperty, PropertyKey.fromString("alluxio.test.property.alias1"));
    assertEquals(mTestProperty, PropertyKey.fromString("alluxio.test.property.alias2"));
    assertEquals(mTestProperty, PropertyKey.fromString(mTestProperty.toString()));
  }

  @Test
  public void equalsTest() throws Exception {
    assertEquals(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo"),
        PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo"));
    assertEquals(PropertyKey.HOME, PropertyKey.HOME);
  }

  @Test
  public void length() throws Exception {
    assertEquals(PropertyKey.Name.HOME.length(), PropertyKey.HOME.length());
  }

  @Test
  public void isValid() throws Exception {
    assertTrue(PropertyKey.isValid(PropertyKey.HOME.toString()));
    assertTrue(PropertyKey
        .isValid(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo").toString()));
    assertFalse(PropertyKey.isValid(""));
    assertFalse(PropertyKey.isValid(" "));
    assertFalse(PropertyKey.isValid("foo"));
    assertFalse(PropertyKey.isValid(PropertyKey.HOME.toString() + "1"));
    assertFalse(PropertyKey.isValid(PropertyKey.HOME.toString().toUpperCase()));
  }

  @Test
  public void aliasIsValid() throws Exception {
    assertTrue(PropertyKey.isValid(mTestProperty.toString()));
    assertTrue(PropertyKey.isValid("alluxio.test.property.alias1"));
    assertTrue(PropertyKey.isValid("alluxio.test.property.alias2"));
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
        assertEquals(e.getMessage(),
            ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
      }
    }
  }

  @Test
  public void formatMasterTieredStoreGlobalAlias() throws Exception {
    assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS,
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0));
    assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS,
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(1));
    assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS,
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(2));
  }

  @Test
  public void formatWorkerTieredStoreAlias() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_ALIAS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsPath() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsQuota() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(2));
  }

  @Test
  public void formatWorkerTieredStoreReservedRatio() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(2));
  }

  @Test
  public void mountTableRootProperties() throws Exception {
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_ALLUXIO,
        PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        PropertyKey.Template.MASTER_MOUNT_TABLE_UFS.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY,
        PropertyKey.Template.MASTER_MOUNT_TABLE_READONLY.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED,
        PropertyKey.Template.MASTER_MOUNT_TABLE_SHARED.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION,
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("root"));
  }

  @Test
  public void isValidParameterized() throws Exception {
    // String parameter
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.foo.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.FoO.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.Fo123.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.FoO.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.option"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.option.foo"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.option.alluxio.foo"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.alluxio"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table..alluxio"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table. .alluxio"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.foo.alluxio1"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.root.option."));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.root.option.foo."));
    // Numeric parameter
    assertTrue(PropertyKey.isValid("alluxio.worker.tieredstore.level1.alias"));
    assertTrue(PropertyKey.isValid("alluxio.worker.tieredstore.level99.alias"));
    assertFalse(PropertyKey.isValid("alluxio.worker.tieredstore.level.alias"));
    assertFalse(PropertyKey.isValid("alluxio.worker.tieredstore.levela.alias"));
  }

  @Test
  public void fromStringParameterized() throws Exception {
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_ALLUXIO,
        PropertyKey.fromString("alluxio.master.mount.table.root.alluxio"));
    assertEquals(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo"),
        PropertyKey.fromString("alluxio.master.mount.table.foo.alluxio"));
  }

  @Test
  public void fromStringParameterizedExceptionThrown() throws Exception {
    String[] wrongKeys = {"alluxio.master.mount.table.root.alluxio1",
        "alluxio.master.mount.table.alluxio", "alluxio.master.mount.table.foo"};
    for (String key : wrongKeys) {
      try {
        PropertyKey.fromString(key);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        assertEquals(e.getMessage(),
            ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
      }
    }
  }

  @Test
  public void isDeprecated() throws Exception {
    assertFalse(PropertyKey.isDeprecated("VERSION"));
    assertTrue(PropertyKey.isDeprecated("MASTER_ADDRESS"));
  }

  @Test
  public void isDeprecatedExceptionThrown() throws Exception {
    assertFalse(PropertyKey.isDeprecated("foo"));
  }
}
