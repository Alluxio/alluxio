/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block descriptor.
 */
@NotThreadSafe
public final class BlockInfo {
  private long mBlockId;
  private long mLength;
  private List<BlockLocation> mLocations = Lists.newArrayList();

  /**
   * Creates a new instance of {@link BlockInfo}.
   */
  public BlockInfo() {}

  /**
   * Creates a new instance of {@link BlockInfo} from a thrift representation.
   *
   * @param blockInfo the thrift representation of a block descriptor
   */
  protected BlockInfo(alluxio.thrift.BlockInfo blockInfo) {
    mBlockId = blockInfo.getBlockId();
    mLength = blockInfo.getLength();
    mLocations = new ArrayList<BlockLocation>();
    for (alluxio.thrift.BlockLocation location : blockInfo.getLocations()) {
      mLocations.add(new BlockLocation(location));
    }
  }

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
   * @return the block descriptor
   */
  public BlockInfo setBlockId(long blockId) {
    mBlockId = blockId;
    return this;
  }

  /**
   * @param length the block length to use
   * @return the block descriptor
   */
  public BlockInfo setLength(long length) {
    mLength = length;
    return this;
  }

  /**
   * @param locations the block locations to use
   * @return the block descriptor
   */
  public BlockInfo setLocations(List<BlockLocation> locations) {
    Preconditions.checkNotNull(locations);
    mLocations = locations;
    return this;
  }

  /**
   * @return thrift representation of the block descriptor
   */
  protected alluxio.thrift.BlockInfo toThrift() {
    List<alluxio.thrift.BlockLocation> locations = new ArrayList<alluxio.thrift.BlockLocation>();
    for (BlockLocation location : mLocations) {
      locations.add(location.toThrift());
    }
    return new alluxio.thrift.BlockInfo(mBlockId, mLength, locations);
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
    return Objects.toStringHelper(this).add("id", mBlockId).add("length", mLength)
        .add("locations", mLocations).toString();
  }
}
