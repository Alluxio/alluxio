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

import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;

/**
 * Abstract block management task implementation.
 */
public abstract class AbstractBlockManagementTask implements BlockManagementTask {
  protected final BlockStore mBlockStore;
  protected final BlockMetadataManager mMetadataManager;
  protected final BlockMetadataEvictorView mEvictorView;
  protected final StoreLoadTracker mLoadTracker;

  /**
   * Creates abstract task implementation.
   *
   * @param blockStore block store
   * @param metadataManager meta manager
   * @param evictorView evictor view
   * @param loadTracker load tracker
   */
  public AbstractBlockManagementTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker) {
    mBlockStore = blockStore;
    mMetadataManager = metadataManager;
    mEvictorView = evictorView;
    mLoadTracker = loadTracker;
  }
}
