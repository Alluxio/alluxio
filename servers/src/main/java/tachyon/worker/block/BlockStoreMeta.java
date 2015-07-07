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

package tachyon.worker.block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This class holds the meta data information of a block store.
 * <p>
 * TODO: use proto buf to represent this information
 */
public class BlockStoreMeta {
  // TODO: the following two fields don't need to be computed on the creation of each
  // {@link BlockStoreMeta} instance.
  private final List<Long> mCapacityBytesOnTiers = new ArrayList<Long>();
  private final Map<Long, Long> mCapacityBytesOnDirs = new HashMap<Long, Long>();

  private final List<Long> mUsedBytesOnTiers = new ArrayList<Long>();
  private final Map<Long, List<Long>> mBlockIdsOnDirs = new HashMap<Long, List<Long>>();
  private final Map<Long, Long> mUsedBytesOnDirs = new HashMap<Long, Long>();
  private final Map<Long, String> mDirPaths = new LinkedHashMap<Long, String>();

  public BlockStoreMeta(BlockMetadataManager manager) {
    Preconditions.checkNotNull(manager);
    for (StorageTier tier : manager.getTiers()) {
      mCapacityBytesOnTiers.add(tier.getCapacityBytes());
      mUsedBytesOnTiers.add(tier.getCapacityBytes() - tier.getAvailableBytes());
      for (StorageDir dir : tier.getStorageDirs()) {
        mBlockIdsOnDirs.put(dir.getStorageDirId(), dir.getBlockIds());
        mCapacityBytesOnDirs.put(dir.getStorageDirId(), dir.getCapacityBytes());
        mUsedBytesOnDirs.put(dir.getStorageDirId(),
            dir.getCapacityBytes() - dir.getAvailableBytes());
        mDirPaths.put(dir.getStorageDirId(), dir.getDirPath());
      }
    }
  }

  public Map<Long, List<Long>> getBlockList() {
    return mBlockIdsOnDirs;
  }

  public long getCapacityBytes() {
    long capacityBytes = 0L;
    for (long capacityBytesOnTier : mCapacityBytesOnTiers) {
      capacityBytes += capacityBytesOnTier;
    }
    return capacityBytes;
  }

  public Map<Long, Long> getCapacityBytesOnDirs() {
    return mCapacityBytesOnDirs;
  }

  public List<Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  public Map<Long, String> getDirPaths() {
    return mDirPaths;
  }

  public int getNumberOfBlocks() {
    int numberOfBlocks = 0;
    for (List blockIds : mBlockIdsOnDirs.values()) {
      numberOfBlocks += blockIds.size();
    }
    return numberOfBlocks;
  }

  public long getUsedBytes() {
    long usedBytes = 0L;
    for (long usedBytesOnTier : mUsedBytesOnTiers) {
      usedBytes += usedBytesOnTier;
    }
    return usedBytes;
  }

  public Map<Long, Long> getUsedBytesOnDirs() {
    return mUsedBytesOnDirs;
  }

  public List<Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }
}
