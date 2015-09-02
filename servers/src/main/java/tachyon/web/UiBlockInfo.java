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

package tachyon.web;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.StorageLevelAlias;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;

public final class UiBlockInfo {
  private final List<String> mLocations = new ArrayList<String>();
  private final long mId;
  private final long mBlockLength;
  private final boolean mInMemory;
  private final long mLastAccessTimeMs;

  public UiBlockInfo(BlockInfo blockInfo) {
    Preconditions.checkNotNull(blockInfo);
    mId = blockInfo.getBlockId();
    mBlockLength = blockInfo.getLength();
    // FIXME: compute this from FileBlockInfo when FileBlockInfo includes MasterBlockInfo
    mInMemory = isInMemory(blockInfo);
    mLastAccessTimeMs = -1;
    addLocations(blockInfo.getLocations());
  }

  public UiBlockInfo(long blockId, long blockLength, long blockLastAccessTimeMs, boolean inMemory) {
    mId = blockId;
    mBlockLength = blockLength;
    mInMemory = inMemory;
    mLastAccessTimeMs = blockLastAccessTimeMs;
  }

  private void addLocations(List<BlockLocation> locations) {
    for (BlockLocation location : locations) {
      mLocations.add(location.getWorkerAddress().getMHost());
    }
  }

  /**
   * @return true if the block is in some worker's memory, false otherwise
   */
  private boolean isInMemory(BlockInfo blockInfo) {
    for (BlockLocation location : blockInfo.getLocations()) {
      if (location.getTier() == StorageLevelAlias.MEM.getValue()) {
        return true;
      }
    }
    return false;
  }

  public long getBlockLength() {
    return mBlockLength;
  }

  public long getID() {
    return mId;
  }

  public boolean inMemory() {
    return mInMemory;
  }

  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  public List<String> getLocations() {
    return mLocations;
  }
}
