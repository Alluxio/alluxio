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
 * The block information.
 */
@PublicApi
@NotThreadSafe
public final class BlockInfo implements Serializable {
  private static final long serialVersionUID = 5646834366222004646L;

  private long mBlockId;
  private long mLength;
  private ArrayList<BlockLocation> mLocations = new ArrayList<>();

  /**
   * Creates a new instance of {@link BlockInfo}.
   */
  public BlockInfo() {}

  /**
   * @return the block id
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the block length
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the block locations
   */
  public List<BlockLocation> getLocations() {
    return mLocations;
  }

  /**
   * @param blockId the block id to use
   * @return the block information
   */
  public BlockInfo setBlockId(long blockId) {
    mBlockId = blockId;
    return this;
  }

  /**
   * @param length the block length to use
   * @return the block information
   */
  public BlockInfo setLength(long length) {
    mLength = length;
    return this;
  }

  /**
   * @param locations the block locations to use
   * @return the block information
   */
  public BlockInfo setLocations(List<BlockLocation> locations) {
    mLocations = new ArrayList<>(Preconditions.checkNotNull(locations, "locations"));
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockInfo)) {
      return false;
    }
    BlockInfo that = (BlockInfo) o;
    return mBlockId == that.mBlockId && mLength == that.mLength
        && mLocations.equals(that.mLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mLength, mLocations);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mBlockId).add("length", mLength)
        .add("locations", mLocations).toString();
  }
}
