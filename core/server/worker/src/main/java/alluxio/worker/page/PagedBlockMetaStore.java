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

import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

/**
 * This class manages metadata for the paged block store.
 */
public class PagedBlockMetaStore extends DefaultPageMetaStore {

  private final PagedBlockStoreTier mTier;
  private final HashMultimap<Long, PageInfo> mBlockToPagesMap = HashMultimap.create();

  protected final IndexedSet<BlockMeta> mBlocks = new IndexedSet<>(INDEX_BLOCK_ID);

  private final HashMultimap<PagedBlockStoreDir, Long> mDirToBlocksMap = HashMultimap.create();

  // this is really an inverse view of mDirToBlocksMap, but the inversion cannot be done w/o
  // a full copy, so this is its own map. care must be taken to keep them in sync.
  private final Map<Long, PagedBlockStoreDir> mBlockToDirMap = new HashMap<>();

  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new CopyOnWriteArrayList<>();

  protected static final IndexDefinition<BlockMeta, Long> INDEX_BLOCK_ID =
      new IndexDefinition<BlockMeta, Long>(true) {
        @Override
        public Long getFieldValue(BlockMeta o) {
          return o.getBlockId();
        }
      };

  /**
   * @param tier the storage tier this metastore manages
   */
  public PagedBlockMetaStore(PagedBlockStoreTier tier) {
    super(tier.getPageStoreDirs());
    mTier = tier;
  }

  /**
   * Adds a new block to the metastore.
   *
   * @param blockId the block ID
   * @param blockSize the size of the block
   * @return block meta
   * @throws WorkerOutOfSpaceException when the worker does not enough space to store this block
   */
  public BlockMeta addBlock(long blockId, long blockSize)
      throws WorkerOutOfSpaceException {
    Preconditions.checkState(!hasBlock(blockId), "Block already exists in dir");

    PagedBlockStoreDir designatedDir = downcast(allocate(String.valueOf(blockId), blockSize));
    PagedBlockMeta blockMeta = new PagedBlockMeta(blockId, designatedDir, blockSize);
    designatedDir.addBlockMeta(blockMeta);
    mBlocks.add(blockMeta);
    BlockStoreLocation location = new BlockStoreLocation(DEFAULT_TIER, designatedDir.getDirIndex());
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onCommitBlock(blockId, location);
      }
    }
    return blockMeta;
  }

  /**
   * Adds a temporary block to the metastore for writing.
   *
   * @param blockId the block ID
   * @param initialBytes the estimated initial size of the block
   * @return temp block meta
   */
  public TempBlockMeta addTempBlock(long blockId, long initialBytes) {
    // todo(bowen): implement
    return null;
  }

  /**
   * Checks if a block is currently being stored in the cache.
   *
   * @param blockId block ID
   * @return true if the block is stored in the cache, false otherwise
   */
  public boolean hasBlock(long blockId) {
    return mBlocks.contains(INDEX_BLOCK_ID, blockId);
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
  public PageStoreDir allocate(String blockIdStr, long pageSize) {
    long blockId = Long.parseLong(blockIdStr);
    if (mBlockToDirMap.containsKey(blockId)) {
      return mBlockToDirMap.get(blockId);
    }
    PagedBlockStoreDir designated = downcast(super.allocate(blockIdStr, pageSize));
    mDirToBlocksMap.put(designated, blockId);
    mBlockToDirMap.put(blockId, designated);
    return designated;
  }

  @Override
  @GuardedBy("getLock()")
  public void addPage(PageId pageId, PageInfo pageInfo) {
    long blockId = Long.parseLong(pageId.getFileId());
    if (!hasBlock(blockId)) {
      throw new BlockDoesNotExistRuntimeException(
          ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(blockId));
    }
    super.addPage(pageId, pageInfo);
    final PagedBlockStoreDir destDir = downcast(pageInfo.getLocalCacheDir());
    mBlockToPagesMap.put(blockId, pageInfo);
    mDirToBlocksMap.put(destDir, blockId);
    mBlockToDirMap.put(blockId, destDir);
  }

  @Override
  @GuardedBy("getLock()")
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    PageInfo pageInfo = super.removePage(pageId);
    long blockId = Long.parseLong(pageId.getFileId());
    final PagedBlockStoreDir dir = downcast(pageInfo.getLocalCacheDir());
    mBlockToPagesMap.remove(blockId, pageInfo);
    if (!mBlockToPagesMap.containsKey(blockId)) {
      mDirToBlocksMap.remove(dir, blockId);
      mBlockToDirMap.remove(blockId, dir);
    }
    boolean lastPageOfBlock = !mBlockToPagesMap.containsKey(blockId);
    if (lastPageOfBlock) {
      BlockStoreLocation location = new BlockStoreLocation(DEFAULT_TIER, dir.getDirIndex());
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onRemoveBlock(blockId, location);
        }
      }
    }
    return pageInfo;
  }

  @Override
  @GuardedBy("getLock()")
  public void reset() {
    super.reset();
    mBlockToPagesMap.clear();
    mDirToBlocksMap.clear();
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
    final List<PagedBlockStoreDir> pagedBlockStoreDirs = mTier.getPagedBlockStoreDirs();
    List<String> dirs = pagedBlockStoreDirs.stream().map(StorageDir::getDirPath)
        .collect(Collectors.toList());
    List<Long> capacityOnDirs = pagedBlockStoreDirs.stream().map(PageStoreDir::getCapacityBytes)
        .collect(Collectors.toList());
    List<Long> usedBytesOnDirs = pagedBlockStoreDirs.stream().map(PageStoreDir::getCachedBytes)
        .collect(Collectors.toList());
    int numBlocks = mBlockToPagesMap.keySet().size();
    return new PagedBlockStoreMeta(dirs, capacityOnDirs, usedBytesOnDirs, numBlocks);
  }

  /**
   * @return detailed store meta including all block locations
   */
  public PagedBlockStoreMeta getStoreMetaFull() {
    final List<PagedBlockStoreDir> pageStoreDirs = mTier.getPagedBlockStoreDirs();
    List<String> dirPaths = pageStoreDirs.stream().map(dir -> dir.getRootPath().toString())
        .collect(Collectors.toList());
    List<Long> capacityOnDirs = pageStoreDirs.stream().map(PageStoreDir::getCapacityBytes)
        .collect(Collectors.toList());
    List<Long> usedBytesOnDirs = pageStoreDirs.stream().map(PageStoreDir::getCachedBytes)
        .collect(Collectors.toList());
    Map<PagedBlockStoreDir, Set<Long>> blockLocations = Multimaps.asMap(mDirToBlocksMap);
    ImmutableMap.Builder<BlockStoreLocation, List<Long>> locationMapBuilder =
        ImmutableMap.builder();
    for (int i = 0; i < dirPaths.size(); i++) {
      BlockStoreLocation location = new BlockStoreLocation(
          DEFAULT_TIER, i, DEFAULT_MEDIUM);
      Set<Long> blocks = blockLocations.get(pageStoreDirs.get(i));
      if (blocks == null) {
        locationMapBuilder.put(location, ImmutableList.of());
        continue;
      }
      locationMapBuilder.put(location, ImmutableList.copyOf(blocks));
    }
    return new PagedBlockStoreMeta(dirPaths, capacityOnDirs, usedBytesOnDirs,
        locationMapBuilder.build());
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
