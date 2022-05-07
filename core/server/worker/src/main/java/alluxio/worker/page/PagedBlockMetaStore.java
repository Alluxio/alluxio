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
import alluxio.client.file.cache.DefaultMetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.resource.LockResource;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockStoreMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

/**
 *  An implementation of MetaStore.
 *  This class maintains the one to many relationship between block ids and page ids
 */
public class PagedBlockMetaStore extends DefaultMetaStore implements BlockStoreMeta {

  public static final String DEFAULT_TIER = "DefaultTier";
  public static final BlockStoreLocation DEFAULT_BLOCK_STORE_LOCATION =
      new BlockStoreLocation(DEFAULT_TIER, 0);
  public static final String DEFAULT_DIR = "DefaultDir";
  public static final DefaultStorageTierAssoc DEFAULT_STORAGE_TIER_ASSOC =
      new DefaultStorageTierAssoc(ImmutableList.of("SSD"));

  private final long mCapacity;
  private final ReentrantReadWriteLock mBlockPageMapLock = new ReentrantReadWriteLock();
  @GuardedBy("mBlockPageMapLock")
  private Map<Long, Set<Long>> mBlockPageMap = new HashMap<>();

  /**
   * Constructor of PagedBlockMetaStore.
   * @param conf alluxio configurations
   */
  public PagedBlockMetaStore(AlluxioConfiguration conf) {
    super(conf);
    mCapacity = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE);
  }

  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    super.addPage(pageId, pageInfo);
    long blockId = Long.parseLong(pageId.getFileId());
    try (LockResource lock = new LockResource(mBlockPageMapLock.writeLock())) {
      mBlockPageMap
          .computeIfAbsent(blockId, k -> new HashSet<>())
          .add(pageId.getPageIndex());
    }
  }

  @Override
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    PageInfo pageInfo = super.removePage(pageId);
    long blockId = Long.parseLong(pageId.getFileId());
    try (LockResource lock = new LockResource(mBlockPageMapLock.writeLock())) {
      mBlockPageMap.computeIfPresent(blockId, (k, pageIndexes) -> {
        pageIndexes.remove(pageId.getPageIndex());
        if (pageIndexes.isEmpty()) {
          return null;
        } else {
          return pageIndexes;
        }
      });
    }
    return pageInfo;
  }

  @Override
  public Map<String, List<Long>> getBlockList() {
    try (LockResource lock = new LockResource(mBlockPageMapLock.readLock())) {
      return ImmutableMap.of(DEFAULT_TIER, ImmutableList.copyOf(mBlockPageMap.keySet()));
    }
  }

  @Override
  public Map<BlockStoreLocation, List<Long>> getBlockListByStorageLocation() {
    try (LockResource lock = new LockResource(mBlockPageMapLock.readLock())) {
      return ImmutableMap.of(DEFAULT_BLOCK_STORE_LOCATION,
          ImmutableList.copyOf(mBlockPageMap.keySet()));
    }
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
    return ImmutableMap.of(new Pair(DEFAULT_TIER, DEFAULT_DIR), mCapacity);
  }

  @Override
  public Map<String, List<String>> getDirectoryPathsOnTiers() {
    return ImmutableMap.of(DEFAULT_TIER, ImmutableList.of(DEFAULT_DIR));
  }

  @Override
  public Map<String, List<String>> getLostStorage() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public int getNumberOfBlocks() {
    return mBlockPageMap.size();
  }

  @Override
  public long getUsedBytes() {
    return bytes();
  }

  @Override
  public Map<String, Long> getUsedBytesOnTiers() {
    return ImmutableMap.of(DEFAULT_TIER, bytes());
  }

  @Override
  public Map<Pair<String, String>, Long> getUsedBytesOnDirs() {
    return ImmutableMap.of(new Pair(DEFAULT_TIER, DEFAULT_DIR), bytes());
  }

  @Override
  public StorageTierAssoc getStorageTierAssoc() {
    return DEFAULT_STORAGE_TIER_ASSOC;
  }
}
