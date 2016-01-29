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
 * The file block descriptor.
 */
public class FileBlockInfo {
  @JsonProperty("blockInfo")
  private BlockInfo mBlockInfo;
  @JsonProperty("offset")
  private long mOffset;
  @JsonProperty("ufsLocations")
  private List<WorkerNetAddress> mUfsLocations;

  /**
   * Creates a new instance of {@link FileBlockInfo}.
   */
  public FileBlockInfo() {}

  /**
   * Creates a new instance of {@link FileBlockInfo}.
   *
   * @param blockInfo the block info to use
   * @param offset the offset to use
   * @param ufsLocations the UFS locations to use
   */
  public FileBlockInfo(BlockInfo blockInfo, long offset, List<WorkerNetAddress> ufsLocations) {
    mBlockInfo = blockInfo;
    mOffset = offset;
    mUfsLocations = ufsLocations;
  }

  /**
   * Creates a new instance of {@link FileBlockInfo} from a thrift representation.
   *
   * @param fileBlockInfo the thrift representation of a block descriptor
   */
  public FileBlockInfo(tachyon.thrift.FileBlockInfo fileBlockInfo) {
    mBlockInfo = new BlockInfo(fileBlockInfo.getBlockInfo());
    mOffset = fileBlockInfo.getOffset();
    mUfsLocations = new ArrayList<WorkerNetAddress>();
    for (tachyon.thrift.WorkerNetAddress ufsLocation : fileBlockInfo.getUfsLocations()) {
      mUfsLocations.add(new WorkerNetAddress(ufsLocation));
    }
  }

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
  public List<WorkerNetAddress> getUfsLocations() {
    return mUfsLocations;
  }

  /**
   * @param blockInfo the block info to use
   */
  public void setBlockInfo(BlockInfo blockInfo) {
    mBlockInfo = blockInfo;
  }

  /**
   * @param offset the offset to use
   */
  public void setOffset(long offset) {
    mOffset = offset;
  }

  /**
   * @param ufsLocations the UFS locations to use
   */
  public void setUfsLocations(List<WorkerNetAddress> ufsLocations) {
    mUfsLocations = ufsLocations;
  }

  /**
   * @return thrift representation of the block descriptor
   */
  public tachyon.thrift.FileBlockInfo toThrift() {
    List<tachyon.thrift.WorkerNetAddress> ufsLocations =
        new ArrayList<tachyon.thrift.WorkerNetAddress>();
    for (WorkerNetAddress ufsLocation : mUfsLocations) {
      ufsLocations.add(ufsLocation.toThrift());
    }
    return new tachyon.thrift.FileBlockInfo(mBlockInfo.toThrift(), mOffset, ufsLocations);
  }
}
