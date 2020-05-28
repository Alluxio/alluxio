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
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;

import java.util.concurrent.ExecutorService;

/**
 * Abstract block management task implementation.
 */
public abstract class AbstractBlockManagementTask implements BlockManagementTask {
  protected final BlockStore mBlockStore;
  protected final BlockMetadataManager mMetadataManager;
  protected final BlockMetadataEvictorView mEvictorView;
  protected final StoreLoadTracker mLoadTracker;
  protected final ExecutorService mExecutor;
  protected final BlockTransferExecutor mTransferExecutor;

  /**
   * Creates abstract task implementation.
   *
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor to use for task execution
   */
  public AbstractBlockManagementTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    mBlockStore = blockStore;
    mMetadataManager = metadataManager;
    mEvictorView = evictorView;
    mLoadTracker = loadTracker;
    mExecutor = executor;
    mTransferExecutor = new BlockTransferExecutor(executor, blockStore, loadTracker,
        ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_BLOCK_TRANSFER_CONCURRENCY_LIMIT));
  }
}
