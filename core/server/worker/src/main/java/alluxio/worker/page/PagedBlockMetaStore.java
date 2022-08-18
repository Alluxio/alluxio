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

import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.allocator.Allocator;
import alluxio.client.file.cache.allocator.HashAllocator;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.quota.CacheScope;
import alluxio.exception.PageNotFoundException;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

/**
 * This class manages metadata for the paged block store.
 */
public class PagedBlockMetaStore implements PageMetaStore {
  private final PageMetaStore mDelegate;
  private final SetMultimap<PagedBlockStoreDir, Long> mDirToBlocksMap =
      Multimaps.synchronizedSetMultimap(HashMultimap.create());
  // this is really an inverse view of mDirToBlocksMap, but the inversion cannot be done w/o
  // a full copy, so this is its own map. care must be taken to keep them in sync.
  private final Map<Long, PagedBlockStoreDir> mBlockToDirMap = new ConcurrentHashMap<>();
  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new CopyOnWriteArrayList<>();

  private class BlockPageAllocator implements Allocator {
    private final Allocator mDelegate;

    private BlockPageAllocator(Allocator delegate) {
      mDelegate = delegate;
    }

    /**
     * Allocates a dir for a page of a block. If any other page of the block is being cached in
     * a dir, use the same dir for this page.
     */
    @Override
    public PageStoreDir allocate(String fileId, long fileLength) {
      long blockId = Long.parseLong(fileId);
      if (mBlockToDirMap.containsKey(blockId)) {
        return mBlockToDirMap.get(blockId);
      }
      return mDelegate.allocate(fileId, fileLength);
    }
  }

  /**
   * @param dirs the storage dirs
   */
  public PagedBlockMetaStore(List<PagedBlockStoreDir> dirs) {
    this(dirs, new HashAllocator(ImmutableList.copyOf(dirs)));
  }

  /**
   * @param dirs dirs
   * @param allocator allocator
   */
  public PagedBlockMetaStore(List<PagedBlockStoreDir> dirs, Allocator allocator) {
    mDelegate = new DefaultPageMetaStore(ImmutableList.copyOf(dirs),
        new BlockPageAllocator(allocator));
  }

  /**
   * Checks if a block is currently being stored in the cache.
   *
   * @param blockId block ID
   * @return true if the block is stored in the cache, false otherwise
   */
  public boolean hasBlock(long blockId) {
    return mBlockToDirMap.containsKey(blockId);
  }

  /**
   * Allocates a page store directory for a page of a block.
   * The implementation additionally guarantees that, as long as there is at least one page of the
   * block is being stored, all subsequent pages always get allocated to the same directory as the
   * firstly allocated page.
   * However, it is unspecified whether the same directory will be chosen for a block, when all
   * of its pages are removed from the page store and then added back.
   *
   * @param blockIdStr the block ID
   * @param pageSize size of the page
   * @return the allocated page store dir
   */
  @Override
  @GuardedBy("getLock()")
  public PageStoreDir allocate(String blockIdStr, long pageSize) {
    return mDelegate.allocate(blockIdStr, pageSize);
  }

  @Override
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    return mDelegate.getPageInfo(pageId);
  }

  @Override
  public ReadWriteLock getLock() {
    return mDelegate.getLock();
  }

  @Override
  public boolean hasPage(PageId pageId) {
    return mDelegate.hasPage(pageId);
  }

  @Override
  @GuardedBy("getLock()")
  public void addPage(PageId pageId, PageInfo pageInfo) {
    long blockId = Long.parseLong(pageId.getFileId());
    mDelegate.addPage(pageId, pageInfo);
    final PagedBlockStoreDir destDir = downcast(pageInfo.getLocalCacheDir());
    mDirToBlocksMap.put(destDir, blockId);
    mBlockToDirMap.put(blockId, destDir);
  }

  @Override
  public Iterator<PageId> getPagesIterator() {
    return mDelegate.getPagesIterator();
  }

  @Override
  public List<PageStoreDir> getStoreDirs() {
    return mDelegate.getStoreDirs();
  }

  @Override
  @GuardedBy("getLock()")
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    long blockId = Long.parseLong(pageId.getFileId());
    PageInfo pageInfo = mDelegate.removePage(pageId);
    final PagedBlockStoreDir dir = downcast(pageInfo.getLocalCacheDir());
    if (dir.getBlockCachedPages(blockId) == 0) { // last page of this block has been removed
      mDirToBlocksMap.remove(dir, blockId);
      mBlockToDirMap.remove(blockId, dir);
      BlockStoreLocation location = dir.getLocation();
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onRemoveBlock(blockId, location);
        }
      }
    }
    return pageInfo;
  }

  @Override
  public long bytes() {
    return mDelegate.bytes();
  }

  @Override
  public long numPages() {
    return mDelegate.numPages();
  }

  @Override
  @GuardedBy("getLock()")
  public void reset() {
    mDelegate.reset();
    mDirToBlocksMap.clear();
    mBlockToDirMap.clear();
  }

  @Override
  public PageInfo evict(CacheScope cacheScope, PageStoreDir pageStoreDir) {
    return mDelegate.evict(cacheScope, pageStoreDir);
  }

  @GuardedBy("getLock()")
  Optional<PagedBlockStoreDir> getDirOfBlock(long blockId) {
    return Optional.ofNullable(mBlockToDirMap.get(blockId));
  }

  /**
   * @param listener listener
   */
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    mBlockStoreEventListeners.add(listener);
  }

  /**
   * @return brief store meta
   */
  public PagedBlockStoreMeta getStoreMeta() {
    final List<PageStoreDir> pageStoreDirs = getStoreDirs();
    ImmutableList.Builder<String> dirPaths = ImmutableList.builder();
    ImmutableList.Builder<Long> capacityOnDirs = ImmutableList.builder();
    ImmutableList.Builder<Long> usedBytesOnDirs = ImmutableList.builder();
    int numBlocks = 0;
    for (PageStoreDir dir : pageStoreDirs) {
      final PagedBlockStoreDir pagedBlockStoreDir = downcast(dir);
      dirPaths.add(pagedBlockStoreDir.getRootPath().toString());
      capacityOnDirs.add(pagedBlockStoreDir.getCapacityBytes());
      usedBytesOnDirs.add(pagedBlockStoreDir.getCachedBytes());
      numBlocks += pagedBlockStoreDir.getNumBlocks();
    }
    return new PagedBlockStoreMeta(dirPaths.build(), capacityOnDirs.build(),
        usedBytesOnDirs.build(), numBlocks);
  }

  /**
   * @return detailed store meta including all block locations
   */
  public PagedBlockStoreMeta getStoreMetaFull() {
    final List<PageStoreDir> pageStoreDirs = getStoreDirs();
    ImmutableList.Builder<String> dirPaths = ImmutableList.builder();
    ImmutableList.Builder<Long> capacityOnDirs = ImmutableList.builder();
    ImmutableList.Builder<Long> usedBytesOnDirs = ImmutableList.builder();
    ImmutableMap.Builder<BlockStoreLocation, List<Long>> blockOnDirs = ImmutableMap.builder();
    Map<PagedBlockStoreDir, Set<Long>> blockLocations = Multimaps.asMap(mDirToBlocksMap);
    for (PageStoreDir pageStoreDir : pageStoreDirs) {
      final PagedBlockStoreDir pagedBlockStoreDir = downcast(pageStoreDir);
      dirPaths.add(pagedBlockStoreDir.getRootPath().toString());
      capacityOnDirs.add(pagedBlockStoreDir.getCapacityBytes());
      usedBytesOnDirs.add(pagedBlockStoreDir.getCachedBytes());
      BlockStoreLocation location = pagedBlockStoreDir.getLocation();
      Set<Long> blocks = blockLocations.getOrDefault(pagedBlockStoreDir, ImmutableSet.of());
      blockOnDirs.put(location, ImmutableList.copyOf(blocks));
    }
    return new PagedBlockStoreMeta(dirPaths.build(), capacityOnDirs.build(),
        usedBytesOnDirs.build(), blockOnDirs.build());
  }

  private static PagedBlockStoreDir downcast(PageStoreDir pageStoreDir) {
    if (pageStoreDir instanceof PagedBlockStoreDir) {
      return (PagedBlockStoreDir) pageStoreDir;
    }
    throw new IllegalArgumentException(
        String.format("Unexpected page store dir type %s, for worker page store it should be %s",
        pageStoreDir.getClass().getSimpleName(), PagedBlockStoreDir.class.getSimpleName()));
  }
}
