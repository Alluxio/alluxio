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

import alluxio.DefaultStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.collections.Pair;
import alluxio.exception.PageNotFoundException;
import alluxio.resource.LockResource;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockStoreMeta;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Metadata of the paged block store.
 */
public class PagedBlockStoreMeta implements BlockStoreMeta {
  public static final String DEFAULT_TIER = "DefaultTier";
  public static final String DEFAULT_DIR = "DefaultDir";
  public static final String DEFAULT_MEDIUM = "SSD";
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

  /**
   * Creates a new snapshot of the state of the paged block store.
   * @param metaStore the meta store
   * @param shouldIncludeBlockIds whether detailed block info should be included in the snapshot. If
   *                              false, {@link #getBlockList} and
   *                              {@link #getBlockListByStorageLocation()} will return {@code null}.
   */
  PagedBlockStoreMeta(PageMetaStore metaStore, boolean shouldIncludeBlockIds) {
    // storage dir list is immutable, so no need to acquire a read lock here
    final List<PageStoreDir> dirs = metaStore.getStoreDirs();
    mDirPaths =
        dirs.stream().map(dir -> dir.getRootPath().toString()).collect(Collectors.toList());
    mCapacityBytesOnDirs =
        dirs.stream().map(PageStoreDir::getCapacityBytes).collect(Collectors.toList());
    mUsedBytesOnDirs =
        dirs.stream().map(PageStoreDir::getCachedBytes).collect(Collectors.toList());
    mCapacity = mCapacityBytesOnDirs.stream().reduce(0L, Long::sum);
    mUsed = mUsedBytesOnDirs.stream().reduce(0L, Long::sum);

    if (shouldIncludeBlockIds) {
      HashMultimap<PageStoreDir, String> dirToBlocksMap = HashMultimap.create();
      try (LockResource lock = new LockResource(metaStore.getLock().readLock())) {
        // TODO(bowen): using iterator is expensive. get block ids from page store dirs directly?
        final Iterator<PageId> pages = metaStore.getPagesIterator();
        while (pages.hasNext()) {
          PageId pageId = pages.next();
          try {
            PageInfo pageInfo = metaStore.getPageInfo(pageId);
            String blockId = pageId.getFileId();
            dirToBlocksMap.put(pageInfo.getLocalCacheDir(), blockId);
          } catch (PageNotFoundException e) {
            // unreachable as we have acquired a read lock, and the page IDs are obtained from
            // the iterator given by the metastore
            throw new IllegalStateException(
                String.format("Unreachable state reached because the page %s which "
                    + "should be present in the page metastore cannot be found", pageId), e);
          }
        }
      }
      Map<PageStoreDir, Set<String>> blockLocations = Multimaps.asMap(dirToBlocksMap);
      ImmutableMap.Builder<BlockStoreLocation, List<Long>> mapBuilder = ImmutableMap.builder();
      ImmutableList.Builder<Long> listBuilder = ImmutableList.builder();
      for (int i = 0; i < dirs.size(); i++) {
        BlockStoreLocation location = new BlockStoreLocation(
            DEFAULT_TIER, i, DEFAULT_MEDIUM);
        Set<String> blocks = blockLocations.get(dirs.get(i));
        if (blocks == null) {
          mapBuilder.put(location, ImmutableList.of());
          continue;
        }
        List<Long> blockIds = blocks.stream().map(Long::parseLong).collect(Collectors.toList());
        mapBuilder.put(location, blockIds);
        listBuilder.addAll(blockIds);
      }
      mBlockLocations = Optional.of(mapBuilder.build());
      List<Long> blocks = listBuilder.build();
      mBlocks = Optional.of(blocks);
      mNumBlocks = blocks.size();
    } else {
      mBlockLocations = Optional.empty();
      mBlocks = Optional.empty();
      try (LockResource lock = new LockResource(metaStore.getLock().readLock())) {
        final Iterator<PageId> pages = metaStore.getPagesIterator();
        mNumBlocks = (int) Streams.stream(pages).map(PageId::getFileId).distinct().count();
      }
    }
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
