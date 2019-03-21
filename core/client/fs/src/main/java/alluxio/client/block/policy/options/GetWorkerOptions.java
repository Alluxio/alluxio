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

package alluxio.client.block.policy.options;

import alluxio.annotation.PublicApi;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.BlockInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.List;

/**
 * Method options for
 * {@link alluxio.client.block.policy.BlockLocationPolicy#getWorker(GetWorkerOptions)}.
 */
@PublicApi
public final class GetWorkerOptions {
  private List<BlockWorkerInfo> mBlockWorkerInfos;
  private BlockInfo mBlockInfo;

  /**
   * @return the default {@link GetWorkerOptions}
   */
  public static GetWorkerOptions defaults() {
    return new GetWorkerOptions();
  }

  /**
   * Creates a new instance with defaults.
   */
  private GetWorkerOptions() {
    mBlockInfo = new BlockInfo();
  }

  /**
   * @return the list of block worker infos
   */
  public BlockInfo getBlockInfo() {
    return mBlockInfo;
  }

  /**
   * @return the list of block worker infos
   */
  public Iterable<BlockWorkerInfo> getBlockWorkerInfos() {
    return mBlockWorkerInfos;
  }

  /**
   * @param blockInfo the block information
   * @return the updated options
   */
  public GetWorkerOptions setBlockInfo(BlockInfo blockInfo) {
    mBlockInfo = blockInfo;
    return this;
  }

  /**
   * @param blockWorkerInfos the block worker infos
   * @return the updated options
   */
  public GetWorkerOptions setBlockWorkerInfos(
      List<BlockWorkerInfo> blockWorkerInfos) {
    mBlockWorkerInfos = blockWorkerInfos;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetWorkerOptions)) {
      return false;
    }
    GetWorkerOptions that = (GetWorkerOptions) o;
    return Objects.equal(mBlockWorkerInfos, that.mBlockWorkerInfos)
        && Objects.equal(mBlockInfo, that.mBlockInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockWorkerInfos, mBlockInfo);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("blockInfo", mBlockInfo)
        .add("blockWorkerInfos", mBlockWorkerInfos)
        .toString();
  }
}
