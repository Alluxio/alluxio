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
import java.util.Collections;
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
 * TODO(bin): Use proto buf to represent this information.
 */
public final class BlockStoreMeta {
  // TODO(bin): The following two fields don't need to be computed on the creation of each
  // {@link BlockStoreMeta} instance.

  /** Mapping from storage tier alias to capacity bytes */
  private final Map<String, Long> mCapacityBytesOnTiers = new HashMap<String, Long>();
  private final Map<Long, Long> mCapacityBytesOnDirs = new HashMap<Long, Long>();

  /** Mapping from storage tier alias to used bytes */
  private final Map<String, Long> mUsedBytesOnTiers = new HashMap<String, Long>();
  private final Map<String, List<Long>> mBlockIdsOnTiers = new HashMap<String, List<Long>>();
  private final Map<Long, Long> mUsedBytesOnDirs = new HashMap<Long, Long>();
  private final Map<Long, String> mDirPaths = new LinkedHashMap<Long, String>();

  public BlockStoreMeta(BlockMetadataManager manager) {
    Preconditions.checkNotNull(manager);
    for (StorageTier tier : manager.getTiers()) {
      mCapacityBytesOnTiers.put(tier.getTierAlias(),
          mCapacityBytesOnTiers.getOrDefault(tier.getTierAlias(), 0L) + tier.getCapacityBytes());
      mUsedBytesOnTiers.put(
          tier.getTierAlias(),
          mUsedBytesOnTiers.getOrDefault(tier.getTierAlias(), 0L)
              + (tier.getCapacityBytes() - tier.getAvailableBytes()));
      List<Long> blockIdsOnTier = new ArrayList<Long>();
      for (StorageDir dir : tier.getStorageDirs()) {
        blockIdsOnTier.addAll(dir.getBlockIds());
        mCapacityBytesOnDirs.put(dir.getStorageDirId(), dir.getCapacityBytes());
        mUsedBytesOnDirs.put(dir.getStorageDirId(),
            dir.getCapacityBytes() - dir.getAvailableBytes());
        mDirPaths.put(dir.getStorageDirId(), dir.getDirPath());
      }
      mBlockIdsOnTiers.put(tier.getTierAlias(), blockIdsOnTier);
    }
  }

  /**
   * @return A mapping from storage tier alias to blocks
   */
  public Map<String, List<Long>> getBlockList() {
    return mBlockIdsOnTiers;
  }

  public long getCapacityBytes() {
    long capacityBytes = 0L;
    for (long capacityBytesOnTier : mCapacityBytesOnTiers.values()) {
      capacityBytes += capacityBytesOnTier;
    }
    return capacityBytes;
  }

  public Map<Long, Long> getCapacityBytesOnDirs() {
    return mCapacityBytesOnDirs;
  }

  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  public Map<Long, String> getDirPaths() {
    return mDirPaths;
  }

  public int getNumberOfBlocks() {
    int numberOfBlocks = 0;
    for (List<Long> blockIds : mBlockIdsOnTiers.values()) {
      numberOfBlocks += blockIds.size();
    }
    return numberOfBlocks;
  }

  public long getUsedBytes() {
    long usedBytes = 0L;
    for (long usedBytesOnTier : mUsedBytesOnTiers.values()) {
      usedBytes += usedBytesOnTier;
    }
    return usedBytes;
  }

  public Map<Long, Long> getUsedBytesOnDirs() {
    return Collections.unmodifiableMap(mUsedBytesOnDirs);
  }

  public Map<String, Long> getUsedBytesOnTiers() {
    return Collections.unmodifiableMap(mUsedBytesOnTiers);
  }
}
