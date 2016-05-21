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

package alluxio.worker.block.evictor;

import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides information about the transfer of a block.
 */
@ThreadSafe
public class BlockTransferInfo {
  private final long mBlockId;
  private final BlockStoreLocation mSrcLocation;
  private final BlockStoreLocation mDstLocation;

  /**
   * Creates a new instance of {@link BlockTransferInfo}.
   *
   * @param blockId the block id
   * @param srcLocation the source {@link BlockStoreLocation}
   * @param dstLocation the destination {@link BlockStoreLocation}
   */
  public BlockTransferInfo(long blockId, BlockStoreLocation srcLocation,
      BlockStoreLocation dstLocation) {
    mBlockId = blockId;
    mSrcLocation = srcLocation;
    mDstLocation = dstLocation;
  }

  /**
   * @return the block id
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the source {@link BlockStoreLocation}
   */
  public BlockStoreLocation getSrcLocation() {
    return mSrcLocation;
  }

  /**
   * @return the destination {@link BlockStoreLocation}
   */
  public BlockStoreLocation getDstLocation() {
    return mDstLocation;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("blockId", mBlockId).add("srcLocation", mSrcLocation)
        .add("dstLocation", mDstLocation).toString();
  }
}
