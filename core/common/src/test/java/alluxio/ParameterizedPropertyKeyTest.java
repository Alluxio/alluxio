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
 * Tests enum type {@link ParameterizedPropertyKey}.
 */
public final class ParameterizedPropertyKeyTest {

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
}
