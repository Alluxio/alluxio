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

public class AlignTaskTest extends BaseTierManagementTaskTest {

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    // Current tier layout could end up swapping 2 blocks concurrently.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES,
        2 * BLOCK_SIZE);
    // Disable promotions to avoid interference.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, false);
    // Initialize the tier layout.
    init();
  }

  @Test
  public void testTierAlignment() throws Exception {
    Random rnd = new Random();
    StorageDir[] dirArray = new StorageDir[] {mTestDir1, mTestDir2, mTestDir3, mTestDir4};

    // Start simulating random load on worker.
    startSimulateLoad();

    // Fill each directory.
    long sessionIdCounter = 1000;
    long blockIdCounter = 1000;
    for (StorageDir dir : dirArray) {
      while (dir.getAvailableBytes() > 0) {
        TieredBlockStoreTestUtils.cache(sessionIdCounter++, blockIdCounter++, BLOCK_SIZE,
            mBlockStore, dir.toBlockStoreLocation(), false);
      }
    }

    // Access blocks randomly.
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

    // Stop the load for align task to continue.
    stopSimulateLoad();

    CommonUtils.waitFor("Tiers to be aligned by a background task.",
        () -> mBlockIterator.aligned(BlockStoreLocation.anyDirInTier(FIRST_TIER_ALIAS),
            BlockStoreLocation.anyDirInTier(SECOND_TIER_ALIAS), BlockOrder.NATURAL, (b) -> false),
        WaitForOptions.defaults().setTimeoutMs(60000));
  }
}
