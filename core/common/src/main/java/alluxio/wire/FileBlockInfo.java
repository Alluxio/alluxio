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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The file block information.
 */
@PublicApi
@NotThreadSafe
public final class FileBlockInfo implements Serializable {
  private static final long serialVersionUID = -3313640897617385301L;

  private BlockInfo mBlockInfo = new BlockInfo();
  private long mOffset;
  private ArrayList<String> mUfsLocations = new ArrayList<>();

  /**
   * Creates a new instance of {@link FileBlockInfo}.
   */
  public FileBlockInfo() {}

  /**
   * @return the block info
   */
  public BlockInfo getBlockInfo() {
    return mBlockInfo;
  }

  /**
   * @return the offset
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the UFS locations
   */
  public List<String> getUfsLocations() {
    return mUfsLocations;
  }

  /**
   * @param blockInfo the block info to use
   * @return the file block information
   */
  public FileBlockInfo setBlockInfo(BlockInfo blockInfo) {
    Preconditions.checkNotNull(blockInfo, "blockInfo");
    mBlockInfo = blockInfo;
    return this;
  }

  /**
   * @param offset the offset to use
   * @return the file block information
   */
  public FileBlockInfo setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  /**
   * @param ufsLocations the UFS locations to use
   * @return the file block information
   */
  public FileBlockInfo setUfsLocations(List<String> ufsLocations) {
    Preconditions.checkNotNull(ufsLocations, "ufsLocations");
    mUfsLocations = new ArrayList<>(ufsLocations);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileBlockInfo)) {
      return false;
    }
    FileBlockInfo that = (FileBlockInfo) o;
    return mBlockInfo.equals(that.mBlockInfo) && mOffset == that.mOffset
        && mUfsLocations.equals(that.mUfsLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockInfo, mOffset, mUfsLocations);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("blockInfo", mBlockInfo).add("offset", mOffset)
        .add("ufsLocations", mUfsLocations).toString();
  }
}
