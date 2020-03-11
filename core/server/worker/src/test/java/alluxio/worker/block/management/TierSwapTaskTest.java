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

package alluxio.worker.block.management;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.annotator.BlockIterator;
import alluxio.worker.block.annotator.BlockOrder;
import alluxio.worker.block.annotator.LRUAnnotator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;

public class TierSwapTaskTest {
  private static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
  private static final String SECOND_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[1];
  private static final long SIMULATE_LOAD_SESSION_ID = 1;
  private static final long SIMULATE_LOAD_BLOCK_ID = 1;
  private static final long BLOCK_SIZE = 100;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private TieredBlockStore mBlockStore;
  private BlockMetadataManager mMetaManager;
  private BlockIterator mBlockIterator;

  private StorageDir mTestDir1;
  private StorageDir mTestDir2;
  private StorageDir mTestDir3;
  private StorageDir mTestDir4;

  private BlockWriter mSimulateWriter;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Use LRU for stronger overlap guarantee.
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS,
        LRUAnnotator.class.getName());
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    // Reserve block size per directory.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_RESERVED_SPACE_BYTES, BLOCK_SIZE);
    // Set timeouts for faster task execution.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME, "100ms");
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_IDLE_SLEEP_TIME, "100ms");
    // Disable move task to avoid interference.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_ENABLED, false);

    File tempFolder = mTestFolder.newFolder();
    TieredBlockStoreTestUtils.setupDefaultConf(tempFolder.getAbsolutePath());
    mBlockStore = new TieredBlockStore();
    Field field = mBlockStore.getClass().getDeclaredField("mMetaManager");
    field.setAccessible(true);
    mMetaManager = (BlockMetadataManager) field.get(mBlockStore);
    mBlockIterator = mMetaManager.getBlockIterator();

    mTestDir1 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(0);
    mTestDir2 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(1);
    mTestDir3 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(1);
    mTestDir4 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(2);
  }

  private void startSimulateLoad() throws Exception {
    mBlockStore.createBlock(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID,
        AllocateOptions.forCreate(0, BlockStoreLocation.anyTier()));
    mSimulateWriter = mBlockStore.getBlockWriter(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID);
  }

  private void stopSimulateLoad() throws Exception {
    mBlockStore.abortBlock(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID);
    mSimulateWriter.close();
  }

  @Test
  public void testOverlapElimination() throws Exception {
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

    // Validate there is overlap. (It's not guaranteed but using LRU helps.)
    Assert.assertTrue(mBlockIterator.overlaps(BlockStoreLocation.anyDirInTier(FIRST_TIER_ALIAS),
        BlockStoreLocation.anyDirInTier(SECOND_TIER_ALIAS), BlockOrder.Natural, (b) -> false));

    // Stop the load for swap task to continue.
    stopSimulateLoad();

    CommonUtils.waitFor("Overlap to be sorted out by background swap task.",
        () -> !mBlockIterator.overlaps(BlockStoreLocation.anyDirInTier(FIRST_TIER_ALIAS),
            BlockStoreLocation.anyDirInTier(SECOND_TIER_ALIAS), BlockOrder.Natural, (b) -> false),
        WaitForOptions.defaults().setTimeoutMs(60000));
  }
}
