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

import static alluxio.worker.page.PagedBlockStoreMeta.DEFAULT_MEDIUM;
import static alluxio.worker.page.PagedBlockStoreMeta.DEFAULT_TIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class PagedBlockStoreMetaTest {
  private static final long PAGE_SIZE = Constants.KB;
  private static final String PAGE_STORE_TYPE = "MEM";
  private static final long PAGE_DIR_CAPACITY_0 = 4 * Constants.KB;
  private static final long PAGE_DIR_CAPACITY_1 = 8 * Constants.KB;
  private static final String PAGE_DIR_PATH_0 = "/test/page/store/0";
  private static final String PAGE_DIR_PATH_0_FULL =
      PathUtils.concatPath(PAGE_DIR_PATH_0, PAGE_STORE_TYPE);
  private static final String PAGE_DIR_PATH_1 = "/test/page/store/1";
  private static final String PAGE_DIR_PATH_1_FULL =
      PathUtils.concatPath(PAGE_DIR_PATH_1, PAGE_STORE_TYPE);

  private PagedBlockMetaStore mPageMetaStore;
  private List<PagedBlockStoreDir> mDirs;

  @Rule
  public final ConfigurationRule mConfigRule = new ConfigurationRule(
      ImmutableMap.of(
          PropertyKey.USER_CLIENT_CACHE_SIZE,
              String.format("%d,%d", PAGE_DIR_CAPACITY_0, PAGE_DIR_CAPACITY_1),
          PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, String.valueOf(PAGE_SIZE),
          PropertyKey.USER_CLIENT_CACHE_DIRS, PAGE_DIR_PATH_0 + "," + PAGE_DIR_PATH_1,
          PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PAGE_STORE_TYPE
      ),
      Configuration.modifiableGlobal()
  );

  @Before
  public void setup() throws Exception {
    CacheManagerOptions cachemanagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());
    mDirs = PagedBlockStoreDir.fromPageStoreDirs(
        PageStoreDir.createPageStoreDirs(cachemanagerOptions));
    mPageMetaStore = new PagedBlockMetaStore(mDirs);
  }

  private void generatePages(int numPages, long parentBlockId, int dirIndex, long pageSize) {
    PagedBlockMeta blockMeta =
        new PagedBlockMeta(parentBlockId, numPages * pageSize, mDirs.get(dirIndex));
    mPageMetaStore.addBlock(blockMeta);
    for (int i = 0; i < numPages; i++) {
      PageId pageId = new PageId(String.valueOf(parentBlockId), i);
      PageInfo pageInfo = new PageInfo(pageId, pageSize, mDirs.get(dirIndex));
      mPageMetaStore.addPage(pageId, pageInfo);
    }
  }

  @Test
  public void blocks() throws Exception {
    int numPages = 4;
    long pageSize = Constants.KB;
    List<Long> blockIdsOnDir0 = ImmutableList.of(1L, 2L);
    List<Long> blockIdsOnDir1 = ImmutableList.of(3L);
    for (long blockId : blockIdsOnDir0) {
      generatePages(numPages, blockId, 0, pageSize);
    }
    for (long blockId : blockIdsOnDir1) {
      generatePages(numPages, blockId, 1, pageSize);
    }
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMetaFull();

    int numBlocks = blockIdsOnDir0.size() + blockIdsOnDir1.size();
    assertEquals(numBlocks, storeMeta.getNumberOfBlocks());

    Map<String, List<Long>> blockList = storeMeta.getBlockList();
    assertNotNull(blockList);
    assertEquals(1, blockList.size());
    assertTrue(blockList.containsKey(DEFAULT_TIER));
    assertEquals(numBlocks, blockList.get(DEFAULT_TIER).size());
    for (long blockId : Iterables.concat(blockIdsOnDir0, blockIdsOnDir1)) {
      assertTrue(blockList.get(DEFAULT_TIER).contains(blockId));
    }

    Map<BlockStoreLocation, List<Long>> blockLocations = storeMeta.getBlockListByStorageLocation();
    assertNotNull(blockLocations);
    assertEquals(2, blockLocations.size());
    final BlockStoreLocation blockLocDir0 = new BlockStoreLocation(DEFAULT_TIER, 0, DEFAULT_MEDIUM);
    final BlockStoreLocation blockLocDir1 = new BlockStoreLocation(DEFAULT_TIER, 1, DEFAULT_MEDIUM);
    assertEquals(blockIdsOnDir0.size(), blockLocations.get(blockLocDir0).size());
    assertEquals(blockIdsOnDir1.size(), blockLocations.get(blockLocDir1).size());
    for (long blockId : blockIdsOnDir0) {
      assertTrue(blockLocations.get(blockLocDir0).contains(blockId));
    }
    for (long blockId : blockIdsOnDir1) {
      assertTrue(blockLocations.get(blockLocDir1).contains(blockId));
    }
  }

  @Test
  public void noDetailedBlockList() {
    int numPages = 4;
    long pageSize = Constants.KB;
    List<Long> blockIds = ImmutableList.of(1L, 2L);
    for (long blockId : blockIds) {
      generatePages(numPages, blockId, 0, pageSize);
    }
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMeta();
    assertEquals(2, storeMeta.getNumberOfBlocks());
    assertNull(storeMeta.getBlockList());
    assertNull(storeMeta.getBlockListByStorageLocation());
  }

  @Test
  public void capacity() {
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMetaFull();

    long totalCapacity = mDirs.stream().map(PageStoreDir::getCapacityBytes).reduce(0L, Long::sum);
    assertEquals(totalCapacity, storeMeta.getCapacityBytes());
    assertEquals(totalCapacity,
        (long) storeMeta.getCapacityBytesOnTiers().get(DEFAULT_TIER));
    Map<Pair<String, String>, Long> capacityByDirs = storeMeta.getCapacityBytesOnDirs();
    assertEquals(2, capacityByDirs.size());
    assertEquals(mDirs.get(0).getCapacityBytes(),
        (long) capacityByDirs.get(new Pair<>(DEFAULT_TIER, PAGE_DIR_PATH_0_FULL)));
    assertEquals(mDirs.get(1).getCapacityBytes(),
        (long) capacityByDirs.get(new Pair<>(DEFAULT_TIER, PAGE_DIR_PATH_1_FULL)));
  }

  @Test
  public void directoryPaths() {
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMetaFull();
    Map<String, List<String>> dirByTiers = storeMeta.getDirectoryPathsOnTiers();
    assertEquals(1, dirByTiers.size());
    assertEquals(
        ImmutableList.of(PAGE_DIR_PATH_0_FULL, PAGE_DIR_PATH_1_FULL),
        dirByTiers.get(DEFAULT_TIER));
  }

  @Test
  public void lostStorage() {
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMetaFull();
    assertEquals(ImmutableMap.of(), storeMeta.getLostStorage());
  }

  @Test
  public void usage() {
    long blockId = 0;
    int numPages = 4;
    long pageSize = Constants.KB;
    generatePages(numPages, blockId, 0, pageSize);
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMetaFull();

    assertEquals(numPages * pageSize, storeMeta.getUsedBytes());
    assertEquals(numPages * pageSize, (long) storeMeta.getUsedBytesOnTiers().get(DEFAULT_TIER));
    Map<Pair<String, String>, Long> usedByDirs = storeMeta.getUsedBytesOnDirs();
    assertEquals(2, usedByDirs.size());
    assertEquals(numPages * pageSize,
        (long) usedByDirs.get(new Pair<>(DEFAULT_TIER, PAGE_DIR_PATH_0_FULL)));
    assertEquals(0,
        (long) usedByDirs.get(new Pair<>(DEFAULT_TIER, PAGE_DIR_PATH_1_FULL)));
  }

  @Test
  public void storageTierAssoc() {
    PagedBlockStoreMeta storeMeta = mPageMetaStore.getStoreMetaFull();
    assertEquals(PagedBlockStoreMeta.DEFAULT_STORAGE_TIER_ASSOC, storeMeta.getStorageTierAssoc());
  }
}
