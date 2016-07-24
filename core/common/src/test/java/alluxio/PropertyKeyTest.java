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
 * Tests enum type {@link PropertyKey}.
 */
public final class PropertyKeyTest {

  /**
   * Tests parsing string to PropertyKey by {@link PropertyKey#fromString}.
   */
  @Test
  public void fromString() throws Exception {
    Assert.assertEquals(PropertyKey.VERSION,
        PropertyKey.fromString(PropertyKey.VERSION.toString()));
  }

  /**
   * Tests format PropertyKey based on templates by {@link PropertyKey#format}.
   */
  @Test
  public void format() throws Exception {
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS,
        PropertyKey.format(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT, 0));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS,
        PropertyKey.format(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT, 1));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS,
        PropertyKey.format(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT, 2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_ALIAS,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO,
        PropertyKey.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 2));
  }
}
