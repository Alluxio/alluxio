/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker.block.evictor;

import java.io.File;
import java.util.Collections;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.allocator.MaxFreeAllocator;

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
