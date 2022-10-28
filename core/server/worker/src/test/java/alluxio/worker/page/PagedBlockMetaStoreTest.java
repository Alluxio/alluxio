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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.allocator.Allocator;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.worker.block.AbstractBlockStoreEventListener;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class PagedBlockMetaStoreTest {
  private static final long PAGE_SIZE = Constants.KB;
  private static final long BLOCK_SIZE = Constants.MB;
  private static final String PAGE_STORE_TYPE = "MEM";
  @Rule
  public ConfigurationRule mConfRule = new ConfigurationRule(
      ImmutableMap.of(
          PropertyKey.WORKER_PAGE_STORE_SIZES, "1MB,1MB",
          PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, String.valueOf(PAGE_SIZE),
          PropertyKey.WORKER_PAGE_STORE_DIRS, "/tmp/1,/tmp/2",
          PropertyKey.WORKER_PAGE_STORE_TYPE, PAGE_STORE_TYPE
      ),
      Configuration.modifiableGlobal());

  private List<PagedBlockStoreDir> mDirs;
  private PagedBlockMetaStore mMetastore;

  @Before
  public void setup() throws Exception {
    CacheManagerOptions cachemanagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());
    mDirs = PagedBlockStoreDir.fromPageStoreDirs(
        PageStoreDir.createPageStoreDirs(cachemanagerOptions));
    mMetastore = new PagedBlockMetaStore(mDirs);
  }

  private static BlockPageId blockPageId(String blockId, long pageIndex) {
    return new BlockPageId(blockId, pageIndex, BLOCK_SIZE);
  }

  @Test
  public void addPage() {
    mMetastore.addBlock(new PagedBlockMeta(1, BLOCK_SIZE, mDirs.get(0)));
    mMetastore.addBlock(new PagedBlockMeta(2, BLOCK_SIZE, mDirs.get(1)));
    addPagesOnDir1(blockPageId("1", 0), blockPageId("1", 1));
    addPagesOnDir2(blockPageId("2", 0));

    assertTrue(mMetastore.hasBlock(1));
    assertTrue(mMetastore.hasBlock(2));
    assertEquals(mDirs.get(0), mMetastore.getBlock(1).get().getDir());
    assertEquals(mDirs.get(1), mMetastore.getBlock(2).get().getDir());
    assertEquals(2, mDirs.get(0).getBlockCachedPages(1));
    assertEquals(1, mDirs.get(1).getBlockCachedPages(2));

    PagedBlockStoreMeta storeMeta = mMetastore.getStoreMetaFull();
    assertEquals(
        ImmutableMap.of(
          mDirs.get(0).getLocation(),
          ImmutableList.of(1L),
          mDirs.get(1).getLocation(),
          ImmutableList.of(2L)
        ),
        storeMeta.getBlockListByStorageLocation());
  }

  @Test
  public void removePage() throws Exception {
    mMetastore.addBlock(new PagedBlockMeta(1, BLOCK_SIZE, mDirs.get(0)));
    mMetastore.addBlock(new PagedBlockMeta(2, BLOCK_SIZE, mDirs.get(1)));
    addPagesOnDir1(blockPageId("1", 0), blockPageId("1", 1));
    addPagesOnDir2(blockPageId("2", 0));

    mMetastore.removePage(blockPageId("1", 0));
    mMetastore.removePage(blockPageId("1", 1));
    mMetastore.removePage(blockPageId("2", 0));

    assertFalse(mMetastore.hasBlock(1));
    assertFalse(mMetastore.hasBlock(2));
    assertEquals(0, mDirs.get(0).getBlockCachedPages(1));
    assertEquals(0, mDirs.get(1).getBlockCachedPages(2));

    PagedBlockStoreMeta storeMeta = mMetastore.getStoreMetaFull();
    assertEquals(
        ImmutableMap.of(
            mDirs.get(0).getLocation(),
            ImmutableList.of(),
            mDirs.get(1).getLocation(),
            ImmutableList.of()
        ),
        storeMeta.getBlockListByStorageLocation());
  }

  @Test
  public void reset() throws Exception {
    mMetastore.addBlock(new PagedBlockMeta(1, BLOCK_SIZE, mDirs.get(0)));
    mMetastore.addBlock(new PagedBlockMeta(2, BLOCK_SIZE, mDirs.get(1)));
    addPagesOnDir1(blockPageId("1", 0), blockPageId("1", 1));
    addPagesOnDir2(blockPageId("2", 0));

    mMetastore.reset();
    PagedBlockStoreMeta storeMeta = mMetastore.getStoreMetaFull();
    assertEquals(
        ImmutableMap.of(
            mDirs.get(0).getLocation(),
            ImmutableList.of(),
            mDirs.get(1).getLocation(),
            ImmutableList.of()
        ),
        storeMeta.getBlockListByStorageLocation());
  }

  @Test
  public void listenerOnRemove() throws PageNotFoundException {
    BlockStoreEventListener listener = new AbstractBlockStoreEventListener() {
      @Override
      public void onRemoveBlock(long blockId, BlockStoreLocation location) {
        assertEquals(1, blockId);
        assertEquals(mDirs.get(0).getLocation(), location);
      }
    };

    mMetastore.registerBlockStoreEventListener(listener);
    PageId pageId = blockPageId("1", 0);
    mMetastore.addBlock(new PagedBlockMeta(1, BLOCK_SIZE, mDirs.get(0)));
    addPagesOnDir1(pageId);
    mMetastore.removePage(pageId);
  }

  @Test
  public void stickyAllocate() {
    String blockId = "1";
    long pageSize = 1;
    BlockPageId page = blockPageId(blockId, 0);
    String fileId = page.getFileId();
    mMetastore = new PagedBlockMetaStore(mDirs, new RandomAllocator(ImmutableList.copyOf(mDirs)));
    PageStoreDir dir = mMetastore.allocate(fileId, pageSize);
    PageInfo pageInfo = new PageInfo(page, pageSize, dir);
    mMetastore.addBlock(new PagedBlockMeta(1, BLOCK_SIZE, (PagedBlockStoreDir) dir));
    mMetastore.addPage(page, pageInfo);
    for (int i = 0; i < 100; i++) {
      assertEquals(dir, mMetastore.allocate(fileId, pageSize));
    }
  }

  @Test
  public void allocatePageIsNotAddPage() {
    String fileId = BlockPageId.fileIdOf(1, BLOCK_SIZE);
    PageStoreDir dir = mMetastore.allocate(fileId, 1);
    assertFalse(dir.hasFile(fileId));
    PagedBlockStoreMeta storeMeta = mMetastore.getStoreMeta();
    assertEquals(0, storeMeta.getNumberOfBlocks());
  }

  private void addPagesOnDir1(PageId... pages) {
    addPages(0, pages);
  }

  private void addPagesOnDir2(PageId... pages) {
    addPages(1, pages);
  }

  private void addPages(int dirIndex, PageId... pages) {
    for (PageId pageId : pages) {
      mMetastore.addPage(pageId, new PageInfo(pageId, 0, mDirs.get(dirIndex)));
    }
  }

  private static class RandomAllocator implements Allocator {
    private final List<PageStoreDir> mDirs;

    public RandomAllocator(List<PageStoreDir> dirs) {
      mDirs = dirs;
    }

    @Override
    public PageStoreDir allocate(String fileId, long fileLength) {
      Random rng = new Random();
      return mDirs.get(rng.nextInt(mDirs.size()));
    }
  }
}
