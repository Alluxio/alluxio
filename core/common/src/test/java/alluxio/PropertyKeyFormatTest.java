package alluxio;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests enum type {@link PropertyKeyFormat}.
 */
public final class PropertyKeyFormatTest {

  /**
   * Tests format PropertyKey based on templates by {@link PropertyKeyFormat#format}.
   */
  @Test
  public void format() throws Exception {
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS,
        PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS,
        PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS,
        PropertyKeyFormat.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT.format(2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_ALIAS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(2));

    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_RESERVED_RATIO,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT.format(0));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_RESERVED_RATIO,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT.format(1));
    Assert.assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_RESERVED_RATIO,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT.format(2));
  }
}