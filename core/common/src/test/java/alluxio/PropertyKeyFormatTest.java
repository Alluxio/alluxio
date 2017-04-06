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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests enum type {@link PropertyKeyFormat}.
 */
public final class PropertyKeyFormatTest {

  @Test
  public void formatMasterTieredStoreGlobalAlias() throws Exception {
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS,
        PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS,
        PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS,
        PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(2));
  }

  @Test
  public void formatWorkerTieredStoreAlias() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_ALIAS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsPath() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsQuota() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(2));
  }

  @Test
  public void formatWorkerTieredStoreReservedRatio() throws Exception {
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT.format(2));
  }
}
