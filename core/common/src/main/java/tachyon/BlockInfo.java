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

package tachyon;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The block descriptor.
 */
public class BlockInfo {
  @JsonProperty("blockId")
  private long mBlockId;
  @JsonProperty("length")
  private long mLength;
  @JsonProperty("locations")
  private List<BlockLocation> mLocations;

  /**
   * Creates a new instance of {@link BlockInfo}.
   */
  public BlockInfo() {}

  /**
   * Creates a new instance of {@link BlockInfo}.
   *
   * @param blockId the block id to use
   * @param length the block length to use
   * @param locations the block locations to use
   */
  public BlockInfo(long blockId, long length, List<BlockLocation> locations) {
    mBlockId = blockId;
    mLength = length;
    mLocations = locations;
  }

  /**
   * Creates a new instance of {@link BlockInfo} from a thrift representation.
   *
   * @param blockInfo the thrift representation of a block descriptor
   */
  public BlockInfo(tachyon.thrift.BlockInfo blockInfo) {
    mBlockId = blockInfo.getBlockId();
    mLength = blockInfo.getLength();
    mLocations = new ArrayList<BlockLocation>();
    for (tachyon.thrift.BlockLocation location : blockInfo.getLocations()) {
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
   */
  public void setBlockId(long blockId) {
    mBlockId = blockId;
  }

  /**
   * @param length the block length to use
   */
  public void setLength(long length) {
    mLength = length;
  }

  /**
   * @param locations the block locations to use
   */
  public void setLocations(List<BlockLocation> locations) {
    mLocations = locations;
  }

  /**
   * @return thrift representation of the block descriptor
   */
  public tachyon.thrift.BlockInfo toThrift() {
    List<tachyon.thrift.BlockLocation> locations = new ArrayList<tachyon.thrift.BlockLocation>();
    for (BlockLocation location : mLocations) {
      locations.add(location.toThrift());
    }
    return new tachyon.thrift.BlockInfo(mBlockId, mLength, locations);
  }
}
