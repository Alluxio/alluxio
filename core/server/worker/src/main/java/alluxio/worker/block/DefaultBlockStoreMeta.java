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

package alluxio.worker.block;

import alluxio.collections.Pair;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class holds the metadata information of a block store.
 * <p>
 * TODO(bin): Use proto buf to represent this information.
 */
@ThreadSafe
public final class DefaultBlockStoreMeta implements BlockStoreMeta {
  // TODO(bin): The following two fields don't need to be computed on the creation of each
  // {@link BlockStoreMeta} instance.

  /** Mapping from storage tier alias to capacity bytes. */
  private final Map<String, Long> mCapacityBytesOnTiers = new HashMap<>();

  /** Mapping from storage tier alias to used bytes. */
  private final Map<String, Long> mUsedBytesOnTiers = new HashMap<>();

  /** Mapping from storage tier alias to capacity bytes. */
  private final Map<String, List<Long>> mBlockIdsOnTiers;

  /** Mapping from storage dir tier and path to total capacity. */
  private final Map<Pair<String, String>, Long> mCapacityBytesOnDirs = new HashMap<>();

  /** Mapping from storage dir tier and path to used bytes. */
  private final Map<Pair<String, String>, Long> mUsedBytesOnDirs = new HashMap<>();

  @Override
  public Map<String, List<Long>> getBlockList() {
    Preconditions.checkNotNull(mBlockIdsOnTiers, "mBlockIdsOnTiers");

    return mBlockIdsOnTiers;
  }

  @Override
  public long getCapacityBytes() {
    long capacityBytes = 0L;
    for (long capacityBytesOnTier : mCapacityBytesOnTiers.values()) {
      capacityBytes += capacityBytesOnTier;
    }
    return capacityBytes;
  }

  @Override
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  @Override
  public Map<Pair<String, String>, Long> getCapacityBytesOnDirs() {
    return mCapacityBytesOnDirs;
  }

  @Override
  public Map<String, List<String>> getDirectoryPathsOnTiers() {
    Map<String, List<String>> pathsOnTiers = new HashMap<>();
    for (Pair<String, String> tierPath : mCapacityBytesOnDirs.keySet()) {
      String tier = tierPath.getFirst();
      if (pathsOnTiers.get(tier) == null) {
        pathsOnTiers.put(tier, new LinkedList<String>());
      }
      pathsOnTiers.get(tier).add(tierPath.getSecond());
    }
    return pathsOnTiers;
  }

  @Override
  public int getNumberOfBlocks() {
    Preconditions.checkNotNull(mBlockIdsOnTiers, "mBlockIdsOnTiers");

    int numberOfBlocks = 0;
    for (List<Long> blockIds : mBlockIdsOnTiers.values()) {
      numberOfBlocks += blockIds.size();
    }
    return numberOfBlocks;
  }

  @Override
  public long getUsedBytes() {
    long usedBytes = 0L;
    for (long usedBytesOnTier : mUsedBytesOnTiers.values()) {
      usedBytes += usedBytesOnTier;
    }
    return usedBytes;
  }

  @Override
  public Map<String, Long> getUsedBytesOnTiers() {
    return Collections.unmodifiableMap(mUsedBytesOnTiers);
  }

  @Override
  public Map<Pair<String, String>, Long> getUsedBytesOnDirs() {
    return mUsedBytesOnDirs;
  }

  /**
   * Creates a new instance of {@link DefaultBlockStoreMeta}.
   *
   * @param manager a block metadata manager handle
   */
  protected DefaultBlockStoreMeta(BlockMetadataManager manager, boolean shouldIncludeBlockIds) {
    Preconditions.checkNotNull(manager, "manager");
    for (StorageTier tier : manager.getTiers()) {
      Long capacityBytes = mCapacityBytesOnTiers.get(tier.getTierAlias());
      Long usedBytes = mUsedBytesOnTiers.get(tier.getTierAlias());
      mCapacityBytesOnTiers.put(tier.getTierAlias(),
          (capacityBytes == null ? 0L : capacityBytes) + tier.getCapacityBytes());
      mUsedBytesOnTiers.put(tier.getTierAlias(),
          (usedBytes == null ? 0L : usedBytes) + (tier.getCapacityBytes() - tier
              .getAvailableBytes()));
      for (StorageDir dir : tier.getStorageDirs()) {
        Pair<String, String> dirKey =
            new Pair<>(tier.getTierAlias(), dir.getDirPath());
        mCapacityBytesOnDirs.put(dirKey, dir.getCapacityBytes());
        mUsedBytesOnDirs.put(dirKey, dir.getCapacityBytes() - dir.getAvailableBytes());
      }
    }

    if (shouldIncludeBlockIds) {
      mBlockIdsOnTiers = new HashMap<>();
      for (StorageTier tier : manager.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          List<Long> blockIds;
          if (mBlockIdsOnTiers.containsKey(tier.getTierAlias())) {
            blockIds = mBlockIdsOnTiers.get(tier.getTierAlias());
          } else {
            blockIds = new ArrayList<>();
            mBlockIdsOnTiers.put(tier.getTierAlias(), blockIds);
          }
          blockIds.addAll(dir.getBlockIds());
        }
      }
    } else {
      mBlockIdsOnTiers = null;
    }
  }
}
