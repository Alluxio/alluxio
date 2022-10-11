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
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

/**
 * This class manages metadata for the paged block store.
 */
public class PagedBlockMetaStore implements PageMetaStore {
  @GuardedBy("getLock()")
  private final PageMetaStore mDelegate;
  @GuardedBy("getLock()")
  private final IndexedSet<PagedBlockMeta> mBlocks =
      new IndexedSet<>(INDEX_BLOCK_ID, INDEX_STORE_DIR);
  private final IndexedSet<PagedTempBlockMeta> mTempBlocks =
      new IndexedSet<>(INDEX_TEMP_BLOCK_ID, INDEX_TEMP_STORE_DIR);

  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new CopyOnWriteArrayList<>();

  private static final IndexDefinition<PagedBlockMeta, Long> INDEX_BLOCK_ID =
      new IndexDefinition<PagedBlockMeta, Long>(true) {
        @Override
        public Long getFieldValue(PagedBlockMeta o) {
          return o.getBlockId();
        }
      };
  private static final IndexDefinition<PagedBlockMeta, PagedBlockStoreDir> INDEX_STORE_DIR =
      new IndexDefinition<PagedBlockMeta, PagedBlockStoreDir>(false) {
        @Override
        public PagedBlockStoreDir getFieldValue(PagedBlockMeta o) {
          return o.getDir();
        }
      };

  private static final IndexDefinition<PagedTempBlockMeta, Long> INDEX_TEMP_BLOCK_ID =
      new IndexDefinition<PagedTempBlockMeta, Long>(true) {
        @Override
        public Long getFieldValue(PagedTempBlockMeta o) {
          return o.getBlockId();
        }
      };
  private static final
      IndexDefinition<PagedTempBlockMeta, PagedBlockStoreDir> INDEX_TEMP_STORE_DIR =
      new IndexDefinition<PagedTempBlockMeta, PagedBlockStoreDir>(false) {
        @Override
        public PagedBlockStoreDir getFieldValue(PagedTempBlockMeta o) {
          return o.getDir();
        }
      };

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
      PagedBlockMeta blockMeta = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
      if (blockMeta != null) {
        return blockMeta.getDir();
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
   * Checks if at least one page of a block is currently being stored in the cache.
   *
   * @param blockId block ID
   * @return true if the block is stored in the cache, false otherwise
   */
  @GuardedBy("getLock().readLock()")
  public boolean hasBlock(long blockId) {
    PagedBlockMeta blockMeta = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
    return blockMeta != null && blockMeta.getDir().getBlockCachedPages(blockId) > 0;
  }

  /**
   * @param blockId
   * @return if
   */
  @GuardedBy("getLock().readLock()")
  public boolean hasTempBlock(long blockId) {
    PagedTempBlockMeta blockMeta = mTempBlocks.getFirstByField(INDEX_TEMP_BLOCK_ID, blockId);
    return blockMeta != null;
  }

  /**
   * Checks if the block is fully cached in the block store.
   *
   * @param blockId block ID
   * @return true if the block has been fully cached, false otherwise
   */
  public boolean hasFullBlock(long blockId) {
    PagedBlockMeta blockMeta = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
    return blockMeta != null
        && blockMeta.getDir().getBlockCachedBytes(blockId) == blockMeta.getBlockSize();
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
  @GuardedBy("getLock().readLock()")
  public PageStoreDir allocate(String blockIdStr, long pageSize) {
    return mDelegate.allocate(blockIdStr, pageSize);
  }

  @Override
  @GuardedBy("getLock().readLock()")
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    return mDelegate.getPageInfo(pageId);
  }

  @Override
  public ReadWriteLock getLock() {
    return mDelegate.getLock();
  }

  @Override
  @GuardedBy("getLock().readLock()")
  public boolean hasPage(PageId pageId) {
    return mDelegate.hasPage(pageId);
  }

  @Override
  @GuardedBy("getLock().writeLock()")
  public void addPage(PageId pageId, PageInfo pageInfo) {
    getBlockMetaOfPage(pageId);
    mDelegate.addPage(pageId, pageInfo);
  }

  /**
   * Gets the block meta for a page of the block.
   * @param pageId the page ID
   * @return block meta
   * @throws BlockDoesNotExistRuntimeException when the block is not being stored in the store
   */
  private PagedBlockMeta getBlockMetaOfPage(PageId pageId) {
    long blockId = Long.parseLong(pageId.getFileId());
    PagedBlockMeta blockMeta = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
    if (blockMeta == null) {
      throw new BlockDoesNotExistRuntimeException(blockId);
    }
    return blockMeta;
  }

  @Override
  @GuardedBy("getLock().writeLock()")
  public void addTempPage(PageId pageId, PageInfo pageInfo) {
    long blockId = Long.parseLong(pageId.getFileId());
    PagedTempBlockMeta blockMeta = mTempBlocks.getFirstByField(INDEX_TEMP_BLOCK_ID, blockId);
    if (blockMeta == null) {
      throw new BlockDoesNotExistRuntimeException(blockId);
    }
    mDelegate.addTempPage(pageId, pageInfo);
    blockMeta.setBlockSize(blockMeta.getBlockSize() + pageInfo.getPageSize());
  }

  /**
   * @param blockId
   * @return the permanent block meta after committing
   */
  public PagedBlockMeta commit(long blockId) {
    PagedTempBlockMeta tempBlockMeta = mTempBlocks.getFirstByField(INDEX_TEMP_BLOCK_ID, blockId);
    if (tempBlockMeta == null) {
      throw new BlockDoesNotExistRuntimeException(blockId);
    }
    PagedBlockMeta blockMeta = new PagedBlockMeta(tempBlockMeta.getBlockId(),
        tempBlockMeta.getBlockSize(), tempBlockMeta.getDir());
    mTempBlocks.remove(tempBlockMeta);
    mBlocks.add(blockMeta);
    return blockMeta;
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
  @GuardedBy("getLock().writeLock()")
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    PagedBlockMeta blockMeta = getBlockMetaOfPage(pageId);
    long blockId = blockMeta.getBlockId();
    PagedBlockStoreDir dir = blockMeta.getDir();
    PageInfo pageInfo = mDelegate.removePage(pageId);
    if (dir.getBlockCachedPages(blockId) == 0) { // last page of this block has been removed
      mBlocks.remove(blockMeta);
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
  @GuardedBy("getLock().readLock()")
  public long bytes() {
    return mDelegate.bytes();
  }

  @Override
  @GuardedBy("getLock().readLock()")
  public long numPages() {
    return mDelegate.numPages();
  }

  @Override
  @GuardedBy("getLock().writeLock()")
  public void reset() {
    mDelegate.reset();
    mBlocks.clear();
  }

  @Override
  @GuardedBy("getLock().readLock()")
  public PageInfo evict(CacheScope cacheScope, PageStoreDir pageStoreDir) {
    return mDelegate.evict(cacheScope, pageStoreDir);
  }

  /**
   * @param blockMeta
   */
  public void addBlock(PagedBlockMeta blockMeta) {
    mBlocks.add(blockMeta);
  }

  /**
   * Adds a temp block for writing. The block is always pinned so that its pages don't get
   * evicted before the block is committed.
   * @param blockMeta the temp block to add
   */
  public void addTempBlock(PagedTempBlockMeta blockMeta) {
    mTempBlocks.add(blockMeta);
    // a temp block always needs to be pinned as a client is actively writing it
    blockMeta.getDir().getEvictor().addPinnedBlock(blockMeta.getBlockId());
  }

  @GuardedBy("getLock().readLock()")
  Optional<PagedBlockMeta> getBlock(long blockId) {
    return Optional.ofNullable(mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId));
  }

  @GuardedBy("getLock().readLock()")
  Optional<PagedTempBlockMeta> getTempBlock(long blockId) {
    return Optional.ofNullable(mTempBlocks.getFirstByField(INDEX_TEMP_BLOCK_ID, blockId));
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
  @GuardedBy("getLock().readLock()")
  public PagedBlockStoreMeta getStoreMetaFull() {
    final List<PageStoreDir> pageStoreDirs = getStoreDirs();
    ImmutableList.Builder<String> dirPaths = ImmutableList.builder();
    ImmutableList.Builder<Long> capacityOnDirs = ImmutableList.builder();
    ImmutableList.Builder<Long> usedBytesOnDirs = ImmutableList.builder();
    ImmutableMap.Builder<BlockStoreLocation, List<Long>> blockOnDirs = ImmutableMap.builder();
    for (PageStoreDir pageStoreDir : pageStoreDirs) {
      final PagedBlockStoreDir pagedBlockStoreDir = downcast(pageStoreDir);
      Set<PagedBlockMeta> blocksOfDir = mBlocks.getByField(INDEX_STORE_DIR, pagedBlockStoreDir);
      dirPaths.add(pagedBlockStoreDir.getRootPath().toString());
      capacityOnDirs.add(pagedBlockStoreDir.getCapacityBytes());
      usedBytesOnDirs.add(pagedBlockStoreDir.getCachedBytes());
      BlockStoreLocation location = pagedBlockStoreDir.getLocation();
      List<Long> blocks = blocksOfDir.stream()
          .map(PagedBlockMeta::getBlockId)
          .collect(Collectors.toList());
      blockOnDirs.put(location, blocks);
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
