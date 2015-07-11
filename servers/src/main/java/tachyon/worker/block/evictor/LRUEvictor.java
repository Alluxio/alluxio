/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.evictor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;

public class LRUEvictor extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
  private static final boolean UNUSED_MAP_VALUE = true;

  private BlockMetadataManagerView mManagerView;

  /**
   * access-ordered {@link java.util.LinkedHashMap} from blockId to {@link #UNUSED_MAP_VALUE}(just a
   * placeholder to occupy the value), acts as a LRU double linked list where most recently accessed
   * element is put at the tail while least recently accessed element is put at the head.
   */
  private Map<Long, Boolean> mLRUCache = Collections
      .synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  /**
   * Construct the LRUEvictor with full view
   *
   * @param fullview of BlockMetadataManager
   * @return LRUEvictor constructed
   */
  public LRUEvictor(BlockMetadataManagerView fullview) {
    mManagerView = Preconditions.checkNotNull(fullview);

    // preload existing blocks loaded by StorageDirView to Evictor
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        //evictable blocks are all blocks for full view
        for (BlockMeta blockMeta : dirView.getEvictableBlocks()) {
          long blockId = blockMeta.getBlockId();
          mLRUCache.put(blockId, UNUSED_MAP_VALUE);
        }
      }
    }
  }

  private boolean alreadyAvailable(long bytesToBeAvailable, BlockStoreLocation location)
      throws IOException {
    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
            return true;
          }
        }
      }
      return false;
    }

    int tierAlias = location.tierAlias();
    StorageTierView tierView = mManagerView.getTierView(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
          return true;
        }
      }
      return false;
    }

    StorageDirView dirView = tierView.getDirView(location.dir());
    return dirView.getAvailableBytes() >= bytesToBeAvailable;
  }

  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataManagerView view) throws IOException {
    mManagerView = view;
    return freeSpace(bytesToBeAvailable, location);
  }

  /**
   * This method should only be accessed by {@link freeSpaceWithView} in this class.
   * Frees space in the given block store location.
   * After eviction, at least one StorageDir in the location
   * has the specific amount of free space after eviction. The location can be a specific
   * StorageDir, or {@link BlockStoreLocation#anyTier} or {@link BlockStoreLocation#anyDirInTier}.
   * The view is generated and passed by the calling {@link BlockStore}.
   *
   * <P>
   * This method returns null if Evictor fails to propose a feasible plan to meet the requirement,
   * or an eviction plan with toMove and toEvict fields to indicate how to free space. If both
   * toMove and toEvict of the plan are empty, it indicates that Evictor has no actions to take and
   * the requirement is already met.
   *
   * @param availableBytes the amount of free space in bytes to be ensured after eviction
   * @param location the location in block store
   * @return an eviction plan (possibly with empty fields) to get the free space, or null if no plan
   *         is feasible
   * @throws IOException if given block location is invalid
   */
  private EvictionPlan freeSpace(long bytesToBeAvailable, BlockStoreLocation location)
      throws IOException {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    EvictionPlan plan = null;

    if (alreadyAvailable(bytesToBeAvailable, location)) {
      plan = new EvictionPlan(toMove, toEvict);
      return plan;
    }

    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();

    Iterator<Map.Entry<Long, Boolean>> it = mLRUCache.entrySet().iterator();
    while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
      long blockId = it.next().getKey();
      try {
        BlockMeta meta = mManagerView.getBlockMeta(blockId); // return null if not evictable
        if (null != meta) {
          BlockStoreLocation dirLocation = meta.getBlockLocation();
          if (dirLocation.belongTo(location)) {
            dirCandidates.add(meta.getParentDir(), blockId, meta.getBlockSize());
          }
        }
      } catch (IOException ioe) {
        LOG.warn("Remove block %d from LRU Cache because %s", blockId, ioe);
        it.remove();
      }
    }

    // enough free space
    if (dirCandidates.candidateSize() >= bytesToBeAvailable) {
      // candidate blockIds for eviction from current tier, sorted from less recently used to more
      // recently used
      toEvict = dirCandidates.candidateBlocks();
      // reverse list so that the more recently used blocks are moved to next tier first
      Collections.reverse(toEvict);
      // TODO: maybe we should abstract the strategy of moving blocks to lower tier
      // move as many blocks to next tier as possible
      List<StorageTierView> tierViewsBelow =
          mManagerView.getTierViewsBelow(
              dirCandidates.candidateDir().getParentTier().getTierAlias());
      for (StorageTierView tierView : tierViewsBelow) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          BlockStoreLocation dest = dirView.toBlockStoreLocation();
          Iterator<Long> blocks = toEvict.iterator();
          long remainBytes = dirView.getAvailableBytes();
          while (blocks.hasNext() && remainBytes >= 0) {
            long blockId = blocks.next();
            BlockMeta blockMeta = mManagerView.getBlockMeta(blockId);
            assert (blockMeta != null); // sanity check;
            long blockSize = blockMeta.getBlockSize();
            if (blockSize <= remainBytes) {
              // the block can be moved to the dir
              toMove.add(new Pair<Long, BlockStoreLocation>(blockId, dest));
              blocks.remove();
              remainBytes -= blockSize;
            }
          }
        }
      }

      // assure all blocks are in the store, if not, remove from plan and lru cache
      long toFree = 0L;
      Iterator<Pair<Long, BlockStoreLocation>> moveIt = toMove.iterator();
      while (moveIt.hasNext()) {
        long id = moveIt.next().getFirst();
        if (!mManagerView.isBlockEvictable(id)) {
          mLRUCache.remove(id);
          moveIt.remove();
        } else {
          toFree += mManagerView.getBlockMeta(id).getBlockSize();
        }
      }
      Iterator<Long> evictIt = toEvict.iterator();
      while (evictIt.hasNext()) {
        long id = evictIt.next();
        if (!mManagerView.isBlockEvictable(id)) {
          mLRUCache.remove(id);
          evictIt.remove();
        } else {
          BlockMeta blockMeta = mManagerView.getBlockMeta(id);
          assert (blockMeta != null); // sanity check;
          toFree += blockMeta.getBlockSize();
        }
      }

      // reassure the plan is feasible
      if (mManagerView.getAvailableBytes(location) + toFree >= bytesToBeAvailable) {
        plan = new EvictionPlan(toMove, toEvict);
      }
    }

    return plan;
  }

  /**
   * Thread safe
   */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    mLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    // Since the temp block has been committed, update Evictor about the new added blocks
    mLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    mLRUCache.remove(blockId);
  }
}
