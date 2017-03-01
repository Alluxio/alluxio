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

package alluxio.client.file.policy.options;

import alluxio.annotation.PublicApi;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.policy.BlockLocationPolicy;

import com.google.common.base.Objects;

/**
 * Method options for
 * {@link BlockLocationPolicy#getWorker(BlockLocationPolicyGetWorkerOptions)}.
 */
@PublicApi
public final class BlockLocationPolicyGetWorkerOptions {
  private Iterable<BlockWorkerInfo> mBlockWorkerInfos;
  private long mBlockId;
  private long mBlockSize;

  /**
   * @return the default {@link BlockLocationPolicyGetWorkerOptions}
   */
  public static BlockLocationPolicyGetWorkerOptions defaults() {
    return new BlockLocationPolicyGetWorkerOptions();
  }

  /**
   * Creates a new instance with defaults.
   */
  private BlockLocationPolicyGetWorkerOptions() {}

  /**
   * @return the list of block worker infos
   */
  public Iterable<BlockWorkerInfo> getBlockWorkerInfos() {
    return mBlockWorkerInfos;
  }

  /**
   * @return the block ID
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @param blockWorkerInfos the block worker infos
   * @return the updated options
   */
  public BlockLocationPolicyGetWorkerOptions setBlockWorkerInfos(
      Iterable<BlockWorkerInfo> blockWorkerInfos) {
    mBlockWorkerInfos = blockWorkerInfos;
    return this;
  }

  /**
   * @param blockId the block ID to set
   * @return the updated options
   */
  public BlockLocationPolicyGetWorkerOptions setBlockId(long blockId) {
    mBlockId = blockId;
    return this;
  }

  /**
   * @param blockSize the block size
   * @return the updated options
   */
  public BlockLocationPolicyGetWorkerOptions setBlockSize(long blockSize) {
    mBlockSize = blockSize;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockLocationPolicyGetWorkerOptions)) {
      return false;
    }
    BlockLocationPolicyGetWorkerOptions that = (BlockLocationPolicyGetWorkerOptions) o;
    return Objects.equal(mBlockWorkerInfos, that.mBlockWorkerInfos)
        && Objects.equal(mBlockId, that.mBlockId)
        && Objects.equal(mBlockSize, that.getBlockSize());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockWorkerInfos, mBlockId, mBlockSize);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockId", mBlockId)
        .add("blockSize", mBlockSize)
        .add("blockWorkerInfos", mBlockWorkerInfos)
        .toString();
  }
}
