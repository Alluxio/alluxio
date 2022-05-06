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

import alluxio.StorageTierAssoc;
import alluxio.client.file.cache.DefaultMetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockStoreMeta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  An implementation of MetaStore.
 *  This class maintains the one to many relationship between block ids and page ids
 */
public class PagedBlockMetaStore extends DefaultMetaStore implements BlockStoreMeta {

  private Map<Long, Set<Long>> mBlockPageMap = new HashMap<>();

  /**
   * Constructor of PagedBlockMetaStore.
   * @param conf alluxio configurations
   */
  public PagedBlockMetaStore(AlluxioConfiguration conf) {
    super(conf);
  }

  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    super.addPage(pageId, pageInfo);
    long blockId = Long.parseLong(pageId.getFileId());
    mBlockPageMap
        .computeIfAbsent(blockId, k -> new HashSet<>())
        .add(pageId.getPageIndex());
  }

  @Override
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    PageInfo pageInfo = super.removePage(pageId);
    long blockId = Long.parseLong(pageId.getFileId());
    mBlockPageMap.computeIfPresent(blockId, (k, pageIndexes) -> {
      pageIndexes.remove(pageId.getPageIndex());
      if (pageIndexes.isEmpty()) {
        return null;
      } else {
        return pageIndexes;
      }
    });
    return pageInfo;
  }

  @Override
  public Map<String, List<Long>> getBlockList() {
    return null;
  }

  @Override
  public Map<BlockStoreLocation, List<Long>> getBlockListByStorageLocation() {
    return null;
  }

  @Override
  public long getCapacityBytes() {
    return 0;
  }

  @Override
  public Map<String, Long> getCapacityBytesOnTiers() {
    return null;
  }

  @Override
  public Map<Pair<String, String>, Long> getCapacityBytesOnDirs() {
    return null;
  }

  @Override
  public Map<String, List<String>> getDirectoryPathsOnTiers() {
    return null;
  }

  @Override
  public Map<String, List<String>> getLostStorage() {
    return null;
  }

  @Override
  public int getNumberOfBlocks() {
    return 0;
  }

  @Override
  public long getUsedBytes() {
    return 0;
  }

  @Override
  public Map<String, Long> getUsedBytesOnTiers() {
    return null;
  }

  @Override
  public Map<Pair<String, String>, Long> getUsedBytesOnDirs() {
    return null;
  }

  @Override
  public StorageTierAssoc getStorageTierAssoc() {
    return null;
  }
}
