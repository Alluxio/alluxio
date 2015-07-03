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
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

public class LRUEvictor extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
  private static final boolean UNUSED_MAP_VALUE = true;

  private final BlockMetadataManager mMeta;

  /**
   * access-ordered {@link java.util.LinkedHashMap} from blockId to {@link #UNUSED_MAP_VALUE}(just a
   * placeholder to occupy the value), acts as a LRU double linked list where most recently accessed
   * element is put at the tail while least recently accessed element is put at the head.
   */
  private Map<Long, Boolean> mLRUCache = Collections
      .synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  public LRUEvictor(BlockMetadataManager meta) {
    mMeta = Preconditions.checkNotNull(meta);

    // preload existing blocks loaded by StorageDir to Evictor
    for (StorageTier tier : mMeta.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        for (long blockId : dir.getBlockIds()) {
          mLRUCache.put(blockId, UNUSED_MAP_VALUE);
        }
      }
    }
  }

  private boolean alreadyAvailable(long bytesToBeAvailable, BlockStoreLocation location)
      throws IOException {
    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mMeta.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          if (dir.getAvailableBytes() >= bytesToBeAvailable) {
            return true;
          }
        }
      }
      return false;
    }

    int tierAlias = location.tierAlias();
    StorageTier tier = mMeta.getTier(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.getAvailableBytes() >= bytesToBeAvailable) {
          return true;
        }
      }
      return false;
    }

    StorageDir dir = tier.getDir(location.dir());
    return dir.getAvailableBytes() >= bytesToBeAvailable;
  }

  @Override
  public EvictionPlan freeSpace(long bytesToBeAvailable, BlockStoreLocation location)
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
        BlockMeta meta = mMeta.getBlockMeta(blockId);

        BlockStoreLocation dirLocation = meta.getBlockLocation();
        if (dirLocation.belongTo(location)) {
          dirCandidates.add(meta.getParentDir(), blockId, meta.getBlockSize());
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
      List<StorageTier> tiersBelow =
          mMeta.getTiersBelow(dirCandidates.candidateDir().getParentTier().getTierAlias());
      for (StorageTier tier : tiersBelow) {
        for (StorageDir dir : tier.getStorageDirs()) {
          BlockStoreLocation dest = dir.toBlockStoreLocation();
          Iterator<Long> blocks = toEvict.iterator();
          long remainBytes = dir.getAvailableBytes();
          while (blocks.hasNext() && remainBytes >= 0) {
            long blockId = blocks.next();
            long blockSize = mMeta.getBlockMeta(blockId).getBlockSize();
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
        if (!mMeta.hasBlockMeta(id)) {
          mLRUCache.remove(id);
          moveIt.remove();
        } else {
          toFree += mMeta.getBlockMeta(id).getBlockSize();
        }
      }
      Iterator<Long> evictIt = toEvict.iterator();
      while (evictIt.hasNext()) {
        long id = evictIt.next();
        if (!mMeta.hasBlockMeta(id)) {
          mLRUCache.remove(id);
          evictIt.remove();
        } else {
          toFree += mMeta.getBlockMeta(id).getBlockSize();
        }
      }

      // reassure the plan is feasible
      if (mMeta.getAvailableBytes(location) + toFree >= bytesToBeAvailable) {
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
