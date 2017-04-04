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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests enum type {@link ParameterizedPropertyKey}.
 */
public final class ParameterizedPropertyKeyTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Test
  public void formatMasterTieredStoreGlobalAlias() throws Exception {
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS,
        ParameterizedPropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS,
        ParameterizedPropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(1));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS,
        ParameterizedPropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(2));
  }

  @Test
  public void formatWorkerTieredStoreAlias() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_ALIAS,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsPath() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsQuota() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(2));
  }

  @Test
  public void formatWorkerTieredStoreReservedRatio() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO,
        ParameterizedPropertyKey.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(2));
  }

  @Test
  public void mountTableRootProperties() throws Exception {
    Assert.assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_ALLUXIO,
        ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO.format("root"));
    Assert.assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_UFS.format("root"));
    Assert.assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY,
        ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_READONLY.format("root"));
    Assert.assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED,
        ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_SHARED.format("root"));
    Assert.assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION,
        ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_OPTION.format("root"));
  }

  @Test
  public void isValid() throws Exception {
    // String parameter
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.root.alluxio"));
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.foo.alluxio"));
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.FoO.alluxio"));
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.Fo123.alluxio"));
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.FoO.alluxio"));
    Assert.assertFalse(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.alluxio"));
    Assert.assertFalse(ParameterizedPropertyKey.isValid("alluxio.master.mount.table..alluxio"));
    Assert.assertFalse(ParameterizedPropertyKey.isValid("alluxio.master.mount.table. .alluxio"));
    Assert.assertFalse(ParameterizedPropertyKey.isValid("alluxio.master.mount.table.foo.alluxio1"));
    // Numeric parameter
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.worker.tieredstore.level1.alias"));
    Assert.assertTrue(ParameterizedPropertyKey.isValid("alluxio.worker.tieredstore.level99.alias"));
    Assert.assertFalse(ParameterizedPropertyKey.isValid("alluxio.worker.tieredstore.level.alias"));
    Assert.assertFalse(ParameterizedPropertyKey.isValid("alluxio.worker.tieredstore.levela.alias"));
  }

  @Test
  public void fromString() throws Exception {
    Assert.assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_ALLUXIO,
        ParameterizedPropertyKey.fromString("alluxio.master.mount.table.root.alluxio"));
    Assert.assertEquals(ParameterizedPropertyKey.MASTER_MOUNT_TABLE_ENTRY_ALLUXIO.format("foo"),
        ParameterizedPropertyKey.fromString("alluxio.master.mount.table.foo.alluxio"));
  }

  @Test
  public void fromStringExceptionThrown() throws Exception {
    String[] wrongKeys = {"", " ", "foo", "alluxio.master.mount.table.root.alluxio1",
        "alluxio.master.mount.table.alluxio", "alluxio.master.mount.table.foo"};
    for (String key : wrongKeys) {
      try {
        ParameterizedPropertyKey.fromString(key);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(e.getMessage(),
            ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
      }
    }
  }
}
