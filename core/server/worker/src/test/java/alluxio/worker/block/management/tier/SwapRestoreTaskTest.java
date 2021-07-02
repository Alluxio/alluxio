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
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.annotator.BlockOrder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class SwapRestoreTaskTest extends BaseTierManagementTaskTest {

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    // Disable move task to avoid interference.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, false);
    // Current tier layout could end up swapping 1 big block.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, BLOCK_SIZE);
    // Initialize the tier layout.
    init();
  }

  @Test
  public void testTierAlignment() throws Exception {
    Random rnd = new Random();

    // Start simulating random load on worker.
    startSimulateLoad();

    // Fill first dir with small blocks.
    long sessionIdCounter = 1000;
    long blockIdCounter = 1000;
    while (mTestDir1.getAvailableBytes() > 0) {
      TieredBlockStoreTestUtils.cache(sessionIdCounter++, blockIdCounter++, SMALL_BLOCK_SIZE,
          mBlockStore, mTestDir1.toBlockStoreLocation(), false);
    }

    // Fill the rest with big blocks.
    StorageDir[] dirArray = new StorageDir[] {mTestDir2, mTestDir3, mTestDir4};
    for (StorageDir dir : dirArray) {
      while (dir.getAvailableBytes() > 0) {
        TieredBlockStoreTestUtils.cache(sessionIdCounter++, blockIdCounter++, BLOCK_SIZE,
            mBlockStore, dir.toBlockStoreLocation(), false);
      }
    }

    // Access big blocks randomly.
    //
    // This will cause swaps from below to the top tier.
    // This, in turn, is expected to exhaust swap space at the top tier
    // which is filled with small blocks.
    for (int i = 0; i < 100; i++) {
      StorageDir dirToAccess = dirArray[rnd.nextInt(dirArray.length)];
      List<Long> blockIdList = dirToAccess.getBlockIds();
      if (!blockIdList.isEmpty()) {
        mBlockStore.accessBlock(sessionIdCounter++,
            blockIdList.get(rnd.nextInt(blockIdList.size())));
      }
    }

    // Validate tiers are not aligned. (It's not guaranteed but using LRU helps.)
    Assert.assertTrue(!mBlockIterator.aligned(BlockStoreLocation.anyDirInTier(FIRST_TIER_ALIAS),
        BlockStoreLocation.anyDirInTier(SECOND_TIER_ALIAS), BlockOrder.NATURAL, (b) -> false));

    // Stop the load for swap task to continue.
    stopSimulateLoad();

    // TODO(ggezer): Validate swap-restore task was activated.
    CommonUtils.waitFor("Tiers to be aligned by background swap task.",
        () -> mBlockIterator.aligned(BlockStoreLocation.anyDirInTier(FIRST_TIER_ALIAS),
            BlockStoreLocation.anyDirInTier(SECOND_TIER_ALIAS), BlockOrder.NATURAL, (b) -> false),
        WaitForOptions.defaults().setTimeoutMs(60000));
  }
}
