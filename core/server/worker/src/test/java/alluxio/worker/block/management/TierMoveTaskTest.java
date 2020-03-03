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
import alluxio.worker.block.BlockAllocationOptions;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.StorageDir;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;

public class TierMoveTaskTest {
  private static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
  private static final String SECOND_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[1];
  private static final long SIMULATE_LOAD_SESSION_ID = 1;
  private static final long SIMULATE_LOAD_BLOCK_ID = 1;
  private static final long BLOCK_SIZE = 100;
  private static final double MOVE_LIMIT = 0.2;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private TieredBlockStore mBlockStore;
  private BlockMetadataManager mMetaManager;

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
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    // Set timeouts for faster task execution.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME, "100ms");
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_IDLE_SLEEP_TIME, "100ms");
    // Disable swap task to avoid interference.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_SWAP_ENABLED, false);
    // Disable reserved space for easy measurement.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_RESERVED_SPACE_BYTES, 0);
    // Set move limit.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_LIMIT, MOVE_LIMIT);

    File tempFolder = mTestFolder.newFolder();
    TieredBlockStoreTestUtils.setupDefaultConf(tempFolder.getAbsolutePath());
    mBlockStore = new TieredBlockStore();
    Field field = mBlockStore.getClass().getDeclaredField("mMetaManager");
    field.setAccessible(true);
    mMetaManager = (BlockMetadataManager) field.get(mBlockStore);

    mTestDir1 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(0);
    mTestDir2 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(1);
    mTestDir3 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(1);
    mTestDir4 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(2);
  }

  private void startSimulateLoad() throws Exception {
    mBlockStore.createBlock(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID,
        BlockAllocationOptions.defaultsForCreate(0, BlockStoreLocation.anyTier()));
    mSimulateWriter = mBlockStore.getBlockWriter(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID);
  }

  private void stopSimulateLoad() throws Exception {
    mBlockStore.abortBlock(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID);
    mSimulateWriter.close();
  }

  @Test
  public void testMoveToHigherTier() throws Exception {
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

    // Calculate the expected available bytes on tier after move task finished.
    long availableBytesLimit =
        (long) (mMetaManager.getTier(FIRST_TIER_ALIAS).getCapacityBytes() * MOVE_LIMIT);

    CommonUtils.waitFor("Higher tier to be filled with blocs from lower tier.",
        () -> mTestDir1.getAvailableBytes() + mTestDir2.getAvailableBytes() == availableBytesLimit,
        WaitForOptions.defaults().setTimeoutMs(60000));
  }
}
