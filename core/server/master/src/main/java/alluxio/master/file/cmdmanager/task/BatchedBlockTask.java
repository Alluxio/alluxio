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

package alluxio.master.file.cmdmanager.task;

import alluxio.client.block.BlockStoreClient;

import com.google.common.base.MoreObjects;

import java.util.List;

/**
 * Class for batched command block tasks.
 */
public class BatchedBlockTask implements BlockTask {
  private final int mBatchId;
  private final int mBatchSize;
  private List<Long> mBlockIds;
  private final BlockStoreClient mClient; // todo: use correct clients to call worker api

  /**
   * Constructor.
   * @param batchId the batch id
   * @param batchSize batch size for the batched command
   * @param blockIds file blockIds
   * @param client the block store client
   */
  public BatchedBlockTask(int batchId, int batchSize,
                          List<Long> blockIds, BlockStoreClient client) {
    mBatchId = batchId;
    mBatchSize = batchSize;
    mBlockIds = blockIds;
    mClient = client;
  }

  @Override
  public void runBlockTask() throws InterruptedException {
    mBlockIds.forEach(id -> {
      //todo: execute api call to load blocks
    });
  }

  @Override
  public String getName() {
    return MoreObjects.toStringHelper(this)
            .add("Name", "BatchedBlockTask")
            .add("batchSize", mBatchSize)
            .add("batchId", mBatchId)
            .toString();
  }

  @Override
  public void stop() {
  }

  @Override
  public long getId() {
    return mBatchId;
  }
}
