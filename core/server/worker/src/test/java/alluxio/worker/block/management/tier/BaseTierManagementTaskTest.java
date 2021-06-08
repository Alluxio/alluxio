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
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.annotator.BlockIterator;
import alluxio.worker.block.annotator.LRUAnnotator;

import org.junit.Rule;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;

/**
 * Provides base functionality for tier-management tests.
 */
public abstract class BaseTierManagementTaskTest {
  protected static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
  protected static final String SECOND_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[1];
  protected static final long SIMULATE_LOAD_SESSION_ID = 1;
  protected static final long SIMULATE_LOAD_BLOCK_ID = 1;
  protected static final long SMALL_BLOCK_SIZE = 10;
  protected static final long BLOCK_SIZE = 100;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  protected TieredBlockStore mBlockStore;
  protected BlockMetadataManager mMetaManager;
  protected BlockIterator mBlockIterator;

  protected StorageDir mTestDir1;
  protected StorageDir mTestDir2;
  protected StorageDir mTestDir3;
  protected StorageDir mTestDir4;

  protected BlockWriter mSimulateWriter;

  /**
   * Sets up all dependencies before a test runs.
   */
  protected void init() throws Exception {
    // Disable reviewer to make sure the allocator behavior stays deterministic
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_CLASS,
            "alluxio.worker.block.reviewer.AcceptingReviewer");
    // Use LRU for stronger overlap guarantee.
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS, LRUAnnotator.class.getName());
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    // Set timeout for faster task execution.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME, "100ms");

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

  /**
   * Stars simulating load on the worker.
   */
  protected void startSimulateLoad() throws Exception {
    mBlockStore.createBlock(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID,
        AllocateOptions.forCreate(0, BlockStoreLocation.anyTier()));
    mSimulateWriter = mBlockStore.getBlockWriter(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID);
  }

  /**
   * Stops simulating load on the worker.
   */
  protected void stopSimulateLoad() throws Exception {
    mBlockStore.abortBlock(SIMULATE_LOAD_SESSION_ID, SIMULATE_LOAD_BLOCK_ID);
    mSimulateWriter.close();
  }
}
