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
import alluxio.exception.PageNotFoundException;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.BlockMeta;

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


  public PagedBlockMetaStore(PagedBlockStoreTier tier) {
    super(tier.getPageStoreDirs());
    mTier = tier;
  }

  public void addBlock(long blockId, long blockSize) {
    mBlocks.add(new PagedBlockMeta(blockId, ))
  }

  @Override
  @GuardedBy("getLock()")
  public void addPage(PageId pageId, PageInfo pageInfo) {
    super.addPage(pageId, pageInfo);
    long blockId = Long.parseLong(pageId.getFileId());
    final PagedBlockStoreDir destDir = downcast(pageInfo.getLocalCacheDir());
    boolean firstPageOfBlock = !destDir.hasBlockMeta(blockId);
    if (firstPageOfBlock) {
      destDir.addBlockMeta();
      BlockStoreLocation location = new BlockStoreLocation(DEFAULT_TIER, destDir.getDirIndex());
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onCommitBlock(blockId, location);
        }
      }
    }
    destDir
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

  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    mBlockStoreEventListeners.add(listener);
  }

  public PagedBlockStoreMeta getStoreMeta() {
    List<String> dirs = getStoreDirs().stream().map(dir -> dir.getRootPath().toString())
        .collect(Collectors.toList());
    List<Long> capacityOnDirs = getStoreDirs().stream().map(PageStoreDir::getCapacityBytes)
        .collect(Collectors.toList());
    List<Long> usedBytesOnDirs = getStoreDirs().stream().map(PageStoreDir::getCachedBytes)
        .collect(Collectors.toList());
    int numBlocks = mBlockToPagesMap.keySet().size();
    return new PagedBlockStoreMeta(dirs, capacityOnDirs, usedBytesOnDirs, numBlocks);
  }

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
