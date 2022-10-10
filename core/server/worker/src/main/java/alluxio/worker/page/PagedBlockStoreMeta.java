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

package alluxio.worker.page;

import alluxio.Constants;
import alluxio.DefaultStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockStoreMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Metadata of the paged block store.
 */
public class PagedBlockStoreMeta implements BlockStoreMeta {
  // Paged block store does not support multiple tiers, so assume everything is in the top tier
  public static final String DEFAULT_TIER =
      Configuration.getString(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS);
  // top tier is conventionally ramdisk
  public static final String DEFAULT_MEDIUM = Constants.MEDIUM_MEM;
  public static final DefaultStorageTierAssoc DEFAULT_STORAGE_TIER_ASSOC =
      new DefaultStorageTierAssoc(ImmutableList.of(DEFAULT_MEDIUM));

  private final long mCapacity;
  private final long mUsed;
  private final int mNumBlocks;
  private final List<String> mDirPaths;
  private final List<Long> mCapacityBytesOnDirs;
  private final List<Long> mUsedBytesOnDirs;
  private final Optional<Map<BlockStoreLocation, List<Long>>> mBlockLocations;
  private final Optional<List<Long>> mBlocks;

  PagedBlockStoreMeta(List<String> storageDirPaths, List<Long> capacityBytesOnDirs,
      List<Long> usedBytesOnDirs, int numBlocks) {
    mDirPaths = storageDirPaths;
    mCapacityBytesOnDirs = capacityBytesOnDirs;
    mUsedBytesOnDirs = usedBytesOnDirs;
    mCapacity = capacityBytesOnDirs.stream().reduce(0L, Long::sum);
    mUsed = usedBytesOnDirs.stream().reduce(0L, Long::sum);
    mNumBlocks = numBlocks;
    mBlocks = Optional.empty();
    mBlockLocations = Optional.empty();
  }

  PagedBlockStoreMeta(List<String> storageDirPaths, List<Long> capacityBytesOnDirs,
      List<Long> usedBytesOnDirs, Map<BlockStoreLocation, List<Long>> blockLocations) {
    mDirPaths = storageDirPaths;
    mCapacityBytesOnDirs = capacityBytesOnDirs;
    mUsedBytesOnDirs = usedBytesOnDirs;
    mCapacity = capacityBytesOnDirs.stream().reduce(0L, Long::sum);
    mUsed = usedBytesOnDirs.stream().reduce(0L, Long::sum);

    mBlockLocations = Optional.of(blockLocations);
    ImmutableList.Builder<Long> blockIdsListBuilder = new ImmutableList.Builder<>();
    blockLocations.values().forEach(blockIdsListBuilder::addAll);
    List<Long> blockIds = blockIdsListBuilder.build();
    mNumBlocks = blockIds.size();
    mBlocks = Optional.of(blockIds);
  }

  @Nullable
  @Override
  public Map<String, List<Long>> getBlockList() {
    return mBlocks.map(blocks -> ImmutableMap.of(DEFAULT_TIER, blocks)).orElse(null);
  }

  @Nullable
  @Override
  public Map<BlockStoreLocation, List<Long>> getBlockListByStorageLocation() {
    return mBlockLocations.orElse(null);
  }

  @Override
  public long getCapacityBytes() {
    return mCapacity;
  }

  @Override
  public Map<String, Long> getCapacityBytesOnTiers() {
    return ImmutableMap.of(DEFAULT_TIER, mCapacity);
  }

  @Override
  public Map<Pair<String, String>, Long> getCapacityBytesOnDirs() {
    return Streams.zip(mDirPaths.stream(), mCapacityBytesOnDirs.stream(), Pair::new)
        .collect(ImmutableMap.toImmutableMap(
           pair -> new Pair<>(DEFAULT_TIER, pair.getFirst()),
           Pair::getSecond
        ));
  }

  @Override
  public Map<String, List<String>> getDirectoryPathsOnTiers() {
    return ImmutableMap.of(DEFAULT_TIER, mDirPaths);
  }

  @Override
  public Map<String, List<String>> getLostStorage() {
    return ImmutableMap.of();
  }

  @Override
  public int getNumberOfBlocks() {
    return mNumBlocks;
  }

  @Override
  public long getUsedBytes() {
    return mUsed;
  }

  @Override
  public Map<String, Long> getUsedBytesOnTiers() {
    return ImmutableMap.of(DEFAULT_TIER, mUsed);
  }

  @Override
  public Map<Pair<String, String>, Long> getUsedBytesOnDirs() {
    return Streams.zip(mDirPaths.stream(), mUsedBytesOnDirs.stream(), Pair::new)
        .collect(ImmutableMap.toImmutableMap(
            pair -> new Pair<>(DEFAULT_TIER, pair.getFirst()),
            Pair::getSecond
        ));
  }

  @Override
  public StorageTierAssoc getStorageTierAssoc() {
    return DEFAULT_STORAGE_TIER_ASSOC;
  }
}
