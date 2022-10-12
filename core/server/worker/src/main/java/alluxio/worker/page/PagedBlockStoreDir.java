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

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A directory storing paged blocks.
 */
public class PagedBlockStoreDir implements PageStoreDir {
  protected final PageStoreDir mDelegate;
  protected final int mIndex;
  // block to pages mapping; if no pages have been added for a block,
  // the block may not exist in this map, but it can still exist in mBlocks
  protected final HashMultimap<Long, PageInfo> mBlockToPagesMap = HashMultimap.create();

  protected final HashMultimap<Long, PageInfo> mTempBlockToPagesMap = HashMultimap.create();

  protected final BlockStoreLocation mLocation;

  protected final BlockPageEvictor mEvictor;

  /**
   * Creates a new dir.
   *
   * @param delegate the page store dir that is delegated to
   * @param index the index of this dir in the tier
   */
  public PagedBlockStoreDir(PageStoreDir delegate, int index) {
    mDelegate = delegate;
    mIndex = index;
    mLocation = new BlockStoreLocation(DEFAULT_TIER, index, DEFAULT_MEDIUM);
    mEvictor = new BlockPageEvictor(delegate.getEvictor());
  }

  /**
   * Creates from a list of {@link PageStoreDir}.
   *
   * @param dirs dirs
   * @return a list of {@link PagedBlockStoreDir}
   */
  public static List<PagedBlockStoreDir> fromPageStoreDirs(List<PageStoreDir> dirs) {
    ImmutableList.Builder<PagedBlockStoreDir> listBuilder = ImmutableList.builder();
    for (int i = 0; i < dirs.size(); i++) {
      listBuilder.add(new PagedBlockStoreDir(dirs.get(i), i));
    }
    return listBuilder.build();
  }

  @Override
  public Path getRootPath() {
    return mDelegate.getRootPath();
  }

  @Override
  public PageStore getPageStore() {
    return mDelegate.getPageStore();
  }

  /**
   * @return index of this directory in the list of all directories
   */
  public int getDirIndex() {
    return mIndex;
  }

  /**
   * @return the block storage location of this directory
   */
  public BlockStoreLocation getLocation() {
    return mLocation;
  }

  /**
   * @return number of blocks being stored in this dir
   */
  public int getNumBlocks() {
    return mBlockToPagesMap.keySet().size();
  }

  @Override
  public long getCapacityBytes() {
    return mDelegate.getCapacityBytes();
  }

  @Override
  public void reset() throws IOException {
    mDelegate.reset();
    mBlockToPagesMap.clear();
    mTempBlockToPagesMap.clear();
    mEvictor.reset();
  }

  @Override
  public void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) throws IOException {
    Consumer<Optional<PageInfo>> wrapper = (optionalPageInfo) -> {
      Optional<PageInfo> mapped = optionalPageInfo.map(pageInfo -> {
        Preconditions.checkArgument(pageInfo.getLocalCacheDir() == mDelegate,
            "scanPages should only return pages under the delegated dir");
        BlockPageId blockPageId;
        try {
          blockPageId = BlockPageId.downcast(pageInfo.getPageId());
        } catch (IllegalArgumentException e) {
          // not a paged block id, return as is
          return pageInfo;
        }
        return new PageInfo(blockPageId, pageInfo.getPageSize(), this);
      });
      pageInfoConsumer.accept(mapped);
    };
    mDelegate.scanPages(wrapper);
  }

  @Override
  public long getCachedBytes() {
    return mDelegate.getCachedBytes();
  }

  @Override
  public void putPage(PageInfo pageInfo) {
    long blockId = BlockPageId.downcast(pageInfo.getPageId()).getBlockId();
    if (mBlockToPagesMap.put(blockId, pageInfo)) {
      mDelegate.putPage(pageInfo);
    }
  }

  @Override
  public void putTempPage(PageInfo pageInfo) {
    long blockId = BlockPageId.downcast(pageInfo.getPageId()).getBlockId();
    if (mTempBlockToPagesMap.put(blockId, pageInfo)) {
      mDelegate.putTempPage(pageInfo);
    }
  }

  @Override
  public boolean putTempFile(String fileId) {
    return mDelegate.putTempFile(fileId);
  }

  @Override
  public boolean reserve(long bytes) {
    // todo(bowen): check constraints and update used bytes, etc.
    return mDelegate.reserve(bytes);
  }

  @Override
  public long deletePage(PageInfo pageInfo) {
    long blockId = BlockPageId.downcast(pageInfo.getPageId()).getBlockId();
    if (mBlockToPagesMap.remove(blockId, pageInfo)) {
      long used = mDelegate.deletePage(pageInfo);
      if (!mBlockToPagesMap.containsKey(blockId)) {
        mEvictor.removePinnedBlock(blockId);
      }
      return used;
    }
    return getCachedBytes();
  }

  @Override
  public long release(long bytes) {
    return mDelegate.release(bytes);
  }

  @Override
  public boolean hasFile(String fileId) {
    return mDelegate.hasFile(fileId);
  }

  @Override
  public boolean hasTempFile(String fileId) {
    return mDelegate.hasTempFile(fileId);
  }

  @Override
  public BlockPageEvictor getEvictor() {
    return mEvictor;
  }

  @Override
  public void close() {
    mDelegate.close();
  }

  @Override
  public void commit(String fileId, String newFileId) throws IOException {
    long blockId = BlockPageId.parseBlockId(fileId);
    Preconditions.checkArgument(
        BlockPageId.parseBlockId(newFileId) == blockId,
        "committing with different block IDs: temp: %s, new: %s", fileId, newFileId);
    mDelegate.commit(fileId, newFileId);
    Set<PageInfo> pages = mTempBlockToPagesMap.removeAll(blockId);
    List<PageInfo> newPages = pages.stream()
        .map(page -> {
          BlockPageId newPageId = new BlockPageId(blockId, page.getPageId().getPageIndex(),
              BlockPageId.parseBlockSize(newFileId));
          return new PageInfo(newPageId, page.getPageSize(), page.getScope(),
              page.getLocalCacheDir());
        })
        .collect(Collectors.toList());
    mBlockToPagesMap.putAll(blockId, newPages);
  }

  @Override
  public void abort(String fileId) throws IOException {
    long blockId = BlockPageId.parseBlockId(fileId);
    mDelegate.abort(fileId);
    mTempBlockToPagesMap.removeAll(blockId);
    mEvictor.removePinnedBlock(blockId);
  }

  /**
   * Gets how many bytes of a block is being cached by this dir.
   *
   * @param blockId the block id
   * @return total size of pages of this block being cached
   */
  public long getBlockCachedBytes(long blockId) {
    return mBlockToPagesMap.get(blockId).stream().map(PageInfo::getPageSize).reduce(0L, Long::sum);
  }

  /**
   * Gets how many bytes of a temp block is being cached by this dir.
   *
   * @param blockId the block id
   * @return total size of pages of this block being cached
   */
  public long getTempBlockCachedBytes(long blockId) {
    return mTempBlockToPagesMap.get(blockId).stream().map(PageInfo::getPageSize)
        .reduce(0L, Long::sum);
  }

  /**
   * Gets how many pages of a block is being cached by this dir.
   *
   * @param blockId the block id
   * @return total number of pages of this block being cached
   */
  public int getBlockCachedPages(long blockId) {
    return mBlockToPagesMap.get(blockId).size();
  }
}
