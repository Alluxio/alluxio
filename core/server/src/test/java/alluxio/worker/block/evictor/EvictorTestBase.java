/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.allocator.MaxFreeAllocator;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

/**
 * Base class for unit tests of evictors.
 *
 * It provides some utilities and initializes an {@link Evictor}, a {@link BlockMetadataManager} and
 * a {@link BlockMetadataManagerView} for a default tiered storage defined in
 * {@link TieredBlockStoreTestUtils#defaultMetadataManagerView(String)}.
 */
public class EvictorTestBase {
  protected static final int SESSION_ID = 2;
  protected static final long BLOCK_ID = 10;

  protected BlockMetadataManager mMetaManager;
  protected BlockMetadataManagerView mManagerView;
  protected Evictor mEvictor;
  protected Allocator mAllocator;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Cache a block to the tiered storage managed by the {@link #mMetaManager}. It's a wrapper around
   * {@link TieredBlockStoreTestUtils#cache}.
   *
   * @param sessionId id of session to cache this block
   * @param blockId id of the block
   * @param bytes length of the block in bytes
   * @param tierLevel tier level for the block in the tiered storage
   * @param dirIndex directory index in tierLevel for the block in the tiered storage
   * @throws Exception when anything goes wrong, should not happen in unit tests
   */
  protected void cache(long sessionId, long blockId, long bytes, int tierLevel, int dirIndex)
      throws Exception {
    TieredBlockStoreTestUtils.cache(sessionId, blockId, bytes, tierLevel, dirIndex, mMetaManager,
        mEvictor);
  }

  /**
   * Initialize an {@link Evictor}, a {@link BlockMetadataManager} and a
   * {@link BlockMetadataManagerView} for a default tiered storage defined in
   * {@link TieredBlockStoreTestUtils#defaultMetadataManagerView(String)}.
   *
   * @param evictorClassName class name of the specific evictor to be tested
   * @throws Exception when anything goes wrong, should not happen in unit tests
   */
  protected void init(String evictorClassName) throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mManagerView =
        new BlockMetadataManagerView(mMetaManager, Collections.<Long>emptySet(),
            Collections.<Long>emptySet());
    Configuration conf = new Configuration();
    conf.set(Constants.WORKER_EVICTOR_CLASS, evictorClassName);
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    mAllocator = Allocator.Factory.create(conf, mManagerView);
    mEvictor = Evictor.Factory.create(conf, mManagerView, mAllocator);
  }
}
