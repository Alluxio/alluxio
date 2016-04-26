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

package alluxio.worker.block;

import alluxio.collections.Pair;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class holds the meta data information of a block store.
 * <p>
 * TODO(bin): Use proto buf to represent this information.
 */
@ThreadSafe
public final class BlockStoreMeta {
  // TODO(bin): The following two fields don't need to be computed on the creation of each
  // {@link BlockStoreMeta} instance.

  /** Mapping from storage tier alias to capacity bytes. */
  private final Map<String, Long> mCapacityBytesOnTiers = new HashMap<String, Long>();

  /** Mapping from storage tier alias to used bytes. */
  private final Map<String, Long> mUsedBytesOnTiers = new HashMap<String, Long>();

  /** Mapping from storage tier alias to stored block ids. */
  private final Map<String, List<Long>> mBlockIdsOnTiers = new HashMap<String, List<Long>>();

  /** Mapping from storage dir tier and path to total capacity. */
  private final Map<Pair<String, String>, Long> mCapacityBytesOnDirs =
      new HashMap<Pair<String, String>, Long>();

  /** Mapping from storage dir tier and path to used bytes. */
  private final Map<Pair<String, String>, Long> mUsedBytesOnDirs =
      new HashMap<Pair<String, String>, Long>();

  /**
   * Creates a new instance of {@link BlockStoreMeta}.
   *
   * @param manager a block metadata manager handle
   */
  public BlockStoreMeta(BlockMetadataManager manager) {
    Preconditions.checkNotNull(manager);
    for (StorageTier tier : manager.getTiers()) {
      Long capacityBytes = mCapacityBytesOnTiers.get(tier.getTierAlias());
      Long usedBytes = mUsedBytesOnTiers.get(tier.getTierAlias());
      mCapacityBytesOnTiers.put(tier.getTierAlias(), (capacityBytes == null ? 0L : capacityBytes)
          + tier.getCapacityBytes());
      mUsedBytesOnTiers.put(
          tier.getTierAlias(),
          (usedBytes == null ? 0L : usedBytes)
              + (tier.getCapacityBytes() - tier.getAvailableBytes()));
      List<Long> blockIdsOnTier = new ArrayList<Long>();
      for (StorageDir dir : tier.getStorageDirs()) {
        blockIdsOnTier.addAll(dir.getBlockIds());
        Pair<String, String> dirKey =
            new Pair<String, String>(tier.getTierAlias(), dir.getDirPath());
        mCapacityBytesOnDirs.put(dirKey, dir.getCapacityBytes());
        mUsedBytesOnDirs.put(dirKey, dir.getCapacityBytes() - dir.getAvailableBytes());
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

  /**
   * @return the capacity in bytes
   */
  public long getCapacityBytes() {
    long capacityBytes = 0L;
    for (long capacityBytesOnTier : mCapacityBytesOnTiers.values()) {
      capacityBytes += capacityBytesOnTier;
    }
    return capacityBytes;
  }

  /**
   * @return a mapping from tier aliases to capacity in bytes
   */
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  /**
   * @return a mapping from tier directory-path pairs to capacity in bytes
   */
  public Map<Pair<String, String>, Long> getCapacityBytesOnDirs() {
    return mCapacityBytesOnDirs;
  }

  /**
   * @return a mapping from tier aliases to directory paths in that tier
   */
  public Map<String, List<String>> getDirectoryPathsOnTiers() {
    Map<String, List<String>> pathsOnTiers = Maps.newHashMap();
    for (Pair<String, String> tierPath : mCapacityBytesOnDirs.keySet()) {
      String tier = tierPath.getFirst();
      if (pathsOnTiers.get(tier) == null) {
        pathsOnTiers.put(tier, new LinkedList<String>());
      }
      pathsOnTiers.get(tier).add(tierPath.getSecond());
    }
    return pathsOnTiers;
  }

  /**
   * @return the number of blocks
   */
  public int getNumberOfBlocks() {
    int numberOfBlocks = 0;
    for (List<Long> blockIds : mBlockIdsOnTiers.values()) {
      numberOfBlocks += blockIds.size();
    }
    return numberOfBlocks;
  }

  /**
   * @return the used capacity in bytes
   */
  public long getUsedBytes() {
    long usedBytes = 0L;
    for (long usedBytesOnTier : mUsedBytesOnTiers.values()) {
      usedBytes += usedBytesOnTier;
    }
    return usedBytes;
  }

  /**
   * @return a mapping from tier aliases to used capacity in bytes
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return Collections.unmodifiableMap(mUsedBytesOnTiers);
  }

  /**
   * @return a mapping from tier directory-path pairs to used capacity in bytes
   */
  public Map<Pair<String, String>, Long> getUsedBytesOnDirs() {
    return mUsedBytesOnDirs;
  }
}
