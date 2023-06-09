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

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.evictor.CacheEvictor;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Evictor for pages that belong to a block.
 * Blocks that are not eligible for eviction because
 * they are pinned, waiting to be persisted, etc., can be temporarily skipped from eviction
 * by calling {@link #addPinnedBlock(long)}, and later calling {@link #removePinnedBlock(long)}
 * makes them eligible for eviction again.
 */
public class BlockPageEvictor implements CacheEvictor {
  private final CacheEvictor mDelegate;
  private final Set<String> mPinnedBlocks = new HashSet<>();
  private final Predicate<PageId> mIsNotPinned =
      (pageId) -> !mPinnedBlocks.contains(pageId.getFileId());

  /**
   * Creates a new block page evictor with an underlying evictor.
   *
   * @param delegate delegated-to cache evictor
   */
  BlockPageEvictor(CacheEvictor delegate) {
    mDelegate = delegate;
  }

  @Override
  public void updateOnGet(PageId pageId) {
    mDelegate.updateOnGet(pageId);
  }

  @Override
  public void updateOnPut(PageId pageId) {
    mDelegate.updateOnPut(pageId);
  }

  @Override
  public void updateOnDelete(PageId pageId) {
    mDelegate.updateOnDelete(pageId);
  }

  @Nullable
  @Override
  public synchronized PageId evict() {
    return mDelegate.evictMatching(mIsNotPinned);
  }

  @Nullable
  @Override
  public synchronized PageId evictMatching(Predicate<PageId> criterion) {
    return mDelegate.evictMatching(pageId -> criterion.test(pageId) && mIsNotPinned.test(pageId));
  }

  @Override
  public synchronized void reset() {
    mDelegate.reset();
    mPinnedBlocks.clear();
  }

  /**
   * Adds a pinned block that is prevented from eviction. All pages belonging to that block
   * will not be eligible for eviction until {@link #removePinnedBlock(long)} is called for that
   * block.
   *
   * @param blockId block id
   * @return true if the block was not already pinned, false otherwise
   */
  public synchronized boolean addPinnedBlock(long blockId) {
    return mPinnedBlocks.add(String.valueOf(blockId));
  }

  /**
   * Removes a pinned block that is prevented from eviction.
   *
   * @param blockId block id
   * @return true if the block was pinned, false otherwise
   */
  public synchronized boolean removePinnedBlock(long blockId) {
    return mPinnedBlocks.remove(String.valueOf(blockId));
  }
}
