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

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides information about the transfer of a block.
 */
@ThreadSafe
public class BlockTransferInfo {
  private static final int INVALID = -1;

  private final long mSrcBlockId;
  private final long mDstBlockId;
  private final BlockStoreLocation mSrcLocation;
  private final BlockStoreLocation mDstLocation;

  private BlockTransferInfo(BlockStoreLocation srcLocation, long srcBlockId,
      BlockStoreLocation dstLocation, long dstBlockId) {
    mSrcLocation = srcLocation;
    mSrcBlockId = srcBlockId;
    mDstLocation = dstLocation;
    mDstBlockId = dstBlockId;
  }

  /**
   * Creates a new instance of {@link BlockTransferInfo} for moving a block.
   *
   * @param srcLocation the source {@link BlockStoreLocation}
   * @param srcBlockId the source block id
   * @param dstLocation the destination {@link BlockStoreLocation}
   * @return the transfer info object
   */
  public static BlockTransferInfo createMove(BlockStoreLocation srcLocation, long srcBlockId,
      BlockStoreLocation dstLocation) {
    return new BlockTransferInfo(srcLocation, srcBlockId, dstLocation, INVALID);
  }

  /**
   * Creates a new instance of {@link BlockTransferInfo} for swapping two blocks.
   *
   * @param srcLocation the source {@link BlockStoreLocation}
   * @param srcBlockId the source block id
   * @param dstLocation the destination {@link BlockStoreLocation}
   * @param dstBlockId the destination block id
   * @return the transfer info object
   */
  public static BlockTransferInfo createSwap(BlockStoreLocation srcLocation, long srcBlockId,
      BlockStoreLocation dstLocation, long dstBlockId) {
    return new BlockTransferInfo(srcLocation, srcBlockId, dstLocation, dstBlockId);
  }

  /**
   * @return the source block id
   */
  public long getSrcBlockId() {
    return mSrcBlockId;
  }

  /**
   * @return the destination block id
   */
  public long getDstBlockId() {
    return mDstBlockId;
  }

  /**
   * @return {@code true} if this tranfer info is for a swap operation
   */
  public boolean isSwap() {
    return mDstBlockId != INVALID;
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
    MoreObjects.ToStringHelper strHelper = MoreObjects.toStringHelper(this);
    if (isSwap()) {
      strHelper.add("TransferType", "SWAP");
      strHelper.add("SrcBlockId", mSrcBlockId);
      strHelper.add("DstBlockId", mDstBlockId);
    } else {
      strHelper.add("TransferType", "MOVE");
      strHelper.add("BlockId", mSrcBlockId);
    }

    strHelper.add("SrcLocation", mSrcLocation);
    strHelper.add("DstLocation", mDstLocation);
    return strHelper.toString();
  }
}
