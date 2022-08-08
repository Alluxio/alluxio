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
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A directory storing paged blocks.
 */
public class PagedBlockStoreDir implements PageStoreDir, StorageDir {
  protected final PageStoreDir mDelegate;
  protected final PagedBlockStoreTier mTier;
  protected final int mIndex;
  // blocks that is currently being managed by this dir
  // note that a block may exist in this map but with no pages added to it
  protected final IndexedSet<BlockMeta> mBlocks = new IndexedSet<>(INDEX_BLOCK_ID);
  // block to pages mapping; if no pages have been added for a block,
  // the block may not exist in this map, but it can still exist in mBlocks
  protected final HashMultimap<BlockMeta, PageInfo> mBlockToPagesMap = HashMultimap.create();

  protected static final IndexDefinition<BlockMeta, Long> INDEX_BLOCK_ID =
      new IndexDefinition<BlockMeta, Long>(true) {
        @Override
        public Long getFieldValue(BlockMeta o) {
          return o.getBlockId();
        }
      };

  protected final BlockStoreLocation mLocation =
      new BlockStoreLocation(DEFAULT_TIER, getDirIndex());

  /**
   * Creates a new dir.
   *
   * @param delegate the page store dir that is delegated to
   * @param tier the tier that the dir belongs to
   * @param index the index of this dir in the tier
   */
  public PagedBlockStoreDir(PageStoreDir delegate, PagedBlockStoreTier tier, int index) {
    mDelegate = delegate;
    mTier = tier;
    mIndex = index;
  }

  @Override
  public Path getRootPath() {
    return mDelegate.getRootPath();
  }

  @Override
  public PageStore getPageStore() {
    return mDelegate.getPageStore();
  }

  @Override
  public int getDirIndex() {
    return mIndex;
  }

  @Override
  public List<Long> getBlockIds() {
    return mBlocks.stream().map(BlockMeta::getBlockId).collect(Collectors.toList());
  }

  @Override
  public List<BlockMeta> getBlocks() {
    return ImmutableList.copyOf(mBlocks);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mBlocks.contains(INDEX_BLOCK_ID, blockId);
  }

  @Override
  public boolean hasTempBlockMeta(long blockId) {
    // todo(bowen): implement this
    return false;
  }

  @Override
  public Optional<BlockMeta> getBlockMeta(long blockId) {
    return Optional.ofNullable(mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId));
  }

  @Override
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    // todo(bowen): return temp block meta
    return Optional.empty();
  }

  @Override
  public void addBlockMeta(BlockMeta blockMeta) throws WorkerOutOfSpaceException {
    Preconditions.checkNotNull(blockMeta, "blockMeta");
    Preconditions.checkState(!hasBlockMeta(blockMeta.getBlockId()),
        ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(blockMeta.getBlockId(), mLocation));
    // todo(bowen): check capacity and quota constraints
    mBlocks.add(blockMeta);
  }

  @Override
  public void addTempBlockMeta(TempBlockMeta tempBlockMeta) {
    // todo(bowen): implement this
  }

  @Override
  public void removeBlockMeta(BlockMeta blockMeta) {
    Preconditions.checkNotNull(blockMeta, "blockMeta");
    // todo(bowen): reclaim space from the removed block
    mBlocks.remove(blockMeta);
  }

  @Override
  public void removeTempBlockMeta(TempBlockMeta tempBlockMeta) {
    // todo(bowen): implement this
  }

  @Override
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize) {
    // todo(bowen): implement this
  }

  @Override
  public void cleanupSessionTempBlocks(long sessionId, List<Long> tempBlockIds) {
    // todo(bowen): implement this
  }

  @Override
  public List<TempBlockMeta> getSessionTempBlocks(long sessionId) {
    // todo(bowen): implement this
    return ImmutableList.of();
  }

  @Override
  public BlockStoreLocation toBlockStoreLocation() {
    return mLocation;
  }

  @Override
  public long getReservedBytes() {
    // todo(bowen): get reserved bytes from PageStoreDir::reserve
    return 0;
  }

  @Override
  public long getCapacityBytes() {
    return mDelegate.getCapacityBytes();
  }

  @Override
  public long getAvailableBytes() {
    return mDelegate.getCapacityBytes() - mDelegate.getCachedBytes();
  }

  @Override
  public long getCommittedBytes() {
    return mDelegate.getCachedBytes();
  }

  @Override
  public String getDirPath() {
    return mDelegate.getRootPath().toString();
  }

  @Override
  public String getDirMedium() {
    return DEFAULT_MEDIUM;
  }

  @Override
  public StorageTier getParentTier() {
    return mTier;
  }

  @Override
  public void reset() throws IOException {
    mDelegate.reset();
  }

  @Override
  public void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) throws IOException {
    mDelegate.scanPages(pageInfoConsumer);
  }

  @Override
  public long getCachedBytes() {
    return mDelegate.getCachedBytes();
  }

  // must have added BlockMeta before calling this
  @Override
  public boolean putPage(PageInfo pageInfo) {
    long blockId = Long.parseLong(String.valueOf(pageInfo.getPageId().getFileId()));
    Preconditions.checkArgument(hasBlockMeta(blockId),
        "Block %s does not exist in dir %s", blockId, mLocation);
    mBlockToPagesMap.put(mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId), pageInfo);
    return mDelegate.putPage(pageInfo);
  }

  @Override
  public boolean putTempFile(String fileId) {
    // todo(bowen): implement this
    return mDelegate.putTempFile(fileId);
  }

  @Override
  public boolean reserve(long bytes) {
    // todo(bowen): check constraints and update used bytes, etc.
    return mDelegate.reserve(bytes);
  }

  @Override
  public long deletePage(PageInfo pageInfo) {
    long blockId = Long.parseLong(String.valueOf(pageInfo.getPageId().getFileId()));
    BlockMeta block = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
    if (block != null) {
      mBlockToPagesMap.remove(block, pageInfo);
      if (!mBlockToPagesMap.containsKey(block)) {
        mBlocks.remove(block);
      }
      return mDelegate.deletePage(pageInfo);
    }
    return mDelegate.getCachedBytes();
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
  public CacheEvictor getEvictor() {
    return mDelegate.getEvictor();
  }

  @Override
  public void close() {
    mDelegate.close();
  }

  /**
   * Gets how many bytes of a block is being cached by this dir.
   *
   * @param blockId the block id
   * @return total size of pages of this block being cached
   */
  public long getBlockCachedBytes(long blockId) {
    Preconditions.checkArgument(hasBlockMeta(blockId),
        "Block %s does not exist in this dir", blockId);
    BlockMeta block = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
    return mBlockToPagesMap.get(block).stream().map(PageInfo::getPageSize).reduce(0L, Long::sum);
  }

  /**
   * Gets how many pages of a block is being cached by this dir.
   *
   * @param blockId the block id
   * @return total number of pages of this block being cached
   */
  public int getBlockCachedPages(long blockId) {
    Preconditions.checkArgument(hasBlockMeta(blockId),
        "Block %s does not exist in this dir", blockId);
    BlockMeta block = mBlocks.getFirstByField(INDEX_BLOCK_ID, blockId);
    return mBlockToPagesMap.get(block).size();
  }
}
