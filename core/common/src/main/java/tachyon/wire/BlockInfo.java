/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.wire;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
