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

package alluxio.worker.block.management.tier;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PromoteTaskTest extends BaseTierManagementTaskTest {
  private static final int MOVE_QUOTA_PERCENT = 80;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    // Disable tier alignment to avoid interference.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, false);
    // Set promotion quota percentage.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_QUOTA_PERCENT,
        MOVE_QUOTA_PERCENT);
    // Initialize the tier layout.
    init();
  }

  @Test
  public void testBlockPromotion() throws Exception {
    // Start simulating random load on worker.
    startSimulateLoad();

    // Fill 'mTestDir3' on lower tier.
    long sessionIdCounter = 1000;
    long blockIdCounter = 1000;
    while (mTestDir3.getAvailableBytes() > 0) {
      TieredBlockStoreTestUtils.cache(sessionIdCounter++, blockIdCounter++, BLOCK_SIZE, mBlockStore,
          mTestDir3.toBlockStoreLocation(), false);
    }

    // Assert that tiers above has no files.
    Assert.assertEquals(0, mTestDir1.getCommittedBytes());
    Assert.assertEquals(0, mTestDir2.getCommittedBytes());

    // Stop the load for move task to continue.
    stopSimulateLoad();

    // Calculate the expected available bytes on tier after promotions finished.
    long usedBytesLimit = (long) (mMetaManager.getTier(FIRST_TIER_ALIAS).getCapacityBytes()
        * (double) MOVE_QUOTA_PERCENT / 100);

    CommonUtils.waitFor("Higher tier to be filled with blocs from lower tier.",
        () -> mTestDir1.getCommittedBytes() + mTestDir2.getCommittedBytes() == usedBytesLimit,
        WaitForOptions.defaults().setTimeoutMs(60000));
  }
}
