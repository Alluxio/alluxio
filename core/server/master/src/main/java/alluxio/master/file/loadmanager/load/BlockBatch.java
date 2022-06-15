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

import alluxio.grpc.FileBlocks;

import java.util.List;

/**
 * Batch of blocks.
 */
public class BlockBatch {
  private final List<Long> mBlocks;
  private final long mBlockSize;
  private final String mUfsPath;
  private final long mBatchId;

  /**
   * Constructor.
   * @param blocks list of blocks
   * @param batchId batchId
   * @param blockSize block size
   * @param ufs ufs path
   */
  public BlockBatch(List<Long> blocks, long batchId, long blockSize, String ufs) {
    mBlocks = blocks;
    mBatchId = batchId;
    mBlockSize = blockSize;
    mUfsPath = ufs;
  }

  /**
   * Get batchId.
   * @return batchId
   */
  public long getBatchId() {
    return mBatchId;
  }

  /**
   * Get block id list.
   * @return list of block ids
   */
  public List<Long> getBlockIds() {
    return mBlocks;
  }

  /**
   * Convert to proto.
   * @return proto format
   */
  public FileBlocks toProto() {
    return FileBlocks
            .newBuilder()
            .addAllBlockId(getBlockIds())
            .setUfsPath(mUfsPath)
            .setBlockSize(mBlockSize)
            .build();
  }
}
