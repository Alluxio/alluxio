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

package alluxio.master.file.loadmanager.load;

import java.util.List;

/**
 * Batch of blocks.
 */
public class BlockBatch {
  private final List<Long> mBlockIds;
  private final long mBatchId;

  /**
   * Constructor.
   * @param blockIds list of block ids
   * @param batchId batchId
   */
  public BlockBatch(List<Long> blockIds, long batchId) {
    mBlockIds = blockIds;
    mBatchId = batchId;
  }

  /**
   * Get batchId.
   * @return batchId
   */
  public long getBatchId() {
    return mBatchId;
  }
}
