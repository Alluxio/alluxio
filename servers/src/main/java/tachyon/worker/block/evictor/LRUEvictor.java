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

  private final BlockMetadataManager mMetaManager;

  /**
   * access-ordered {@link java.util.LinkedHashMap} from blockId to {@link #UNUSED_MAP_VALUE}(just a
   * placeholder to occupy the value), acts as a LRU double linked list where most recently accessed
   * element is put at the tail while least recently accessed element is put at the head.
   */
  private Map<Long, Boolean> mLRUCache = Collections
      .synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  public LRUEvictor(BlockMetadataManager meta) {
    mMetaManager = Preconditions.checkNotNull(meta);

    // preload existing blocks loaded by StorageDir to Evictor
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        for (long blockId : dir.getBlockIds()) {
          mLRUCache.put(blockId, UNUSED_MAP_VALUE);
        }
      }
    }
  }

  /**
   * @return a StorageDir in the range of location that already has availableBytes larger than
   *         bytesToBeAvailable, otherwise null
   */
  private StorageDir selectDirWithRequestedSpace(long bytesToBeAvailable,
      BlockStoreLocation location) throws IOException {
    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mMetaManager.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          if (dir.getAvailableBytes() >= bytesToBeAvailable) {
            return dir;
          }
        }
      }
      return null;
    }

    int tierAlias = location.tierAlias();
    StorageTier tier = mMetaManager.getTier(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.getAvailableBytes() >= bytesToBeAvailable) {
          return dir;
        }
      }
      return null;
    }

    StorageDir dir = tier.getDir(location.dir());
    return (dir.getAvailableBytes() >= bytesToBeAvailable) ? dir : null;
  }

  /**
   * A recursive implementation of cascading LRU eviction.
   *
   * It will try to free space in next tier to transfer blocks there, if the next tier does not have
   * enough free space to hold the blocks, the next next tier will be tried and so on until the
   * bottom tier is reached, if blocks can not even be transferred to the bottom tier, they will be
   * evicted, otherwise, only blocks to be freed in the bottom tier will be evicted.
   *
   * this method is only used in {@link #freeSpace(long, tachyon.worker.block.BlockStoreLocation)}
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location target location to evict blocks from
   * @param plan the plan to be recursively updated, is empty when first called in
   *        {@link #freeSpace(long, tachyon.worker.block.BlockStoreLocation)}
   * @return the first StorageDir in the range of location to evict/move bytes from, or null if
   *         there is no plan
   */
  private StorageDir cascadingEvict(long bytesToBeAvailable, BlockStoreLocation location,
      EvictionPlan plan) throws IOException {

    // 1. if bytesToBeAvailable can already be satisfied without eviction, return emtpy plan
    StorageDir candidateDir = selectDirWithRequestedSpace(bytesToBeAvailable, location);
    if (candidateDir != null) {
      return candidateDir;
    }

    // 2. iterate over blocks in LRU order until we find a dir that is in the range of location and
    // can satisfy bytesToBeAvailable after evicting its blocks iterated so far
    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();
    Iterator<Map.Entry<Long, Boolean>> it = mLRUCache.entrySet().iterator();
    while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
      long blockId = it.next().getKey();
      try {
        BlockMeta block = mMetaManager.getBlockMeta(blockId);
        if (block.getBlockLocation().belongTo(location)) {
          dirCandidates.add(block.getParentDir(), blockId, block.getBlockSize());
        }
      } catch (IOException ioe) {
        LOG.warn("Remove block {} from LRU Cache because {}", blockId, ioe);
        it.remove();
      }
    }

    // 3. have no eviction plan
    if (dirCandidates.candidateSize() < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to free space in next tier to move candidate blocks there, evict
    // blocks only when it can not be moved to next tiers
    candidateDir = dirCandidates.candidateDir();
    List<Long> candidateBlocks = dirCandidates.candidateBlocks();
    List<StorageTier> tiersBelow =
        mMetaManager.getTiersBelow(candidateDir.getParentTier().getTierAlias());
    // find a dir in below tiers to transfer blocks there, from top tier to bottom tier
    StorageDir candidateNextDir = null;
    for (StorageTier tier : tiersBelow) {
      candidateNextDir =
          cascadingEvict(dirCandidates.candidateSize(),
              BlockStoreLocation.anyDirInTier(tier.getTierAlias()), plan);
      if (candidateNextDir != null) {
        break;
      }
    }
    if (candidateNextDir == null) {
      // nowhere to transfer blocks to, so evict them
      plan.toEvict().addAll(candidateBlocks);
    } else {
      BlockStoreLocation dest = candidateNextDir.toBlockStoreLocation();
      for (long block : candidateBlocks) {
        plan.toMove().add(new Pair<Long, BlockStoreLocation>(block, dest));
      }
    }
    return candidateDir;
  }

  @Override
  public EvictionPlan freeSpace(long bytesToBeAvailable, BlockStoreLocation location)
      throws IOException {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    EvictionPlan plan = new EvictionPlan(toMove, toEvict);
    StorageDir candidateDir = cascadingEvict(bytesToBeAvailable, location, plan);

    if (candidateDir == null) {
      return null;
    }
    if (plan.isEmpty()) {
      return plan;
    }

    // assure all blocks are in the store, if not, remove from plan and lru cache
    Iterator<Pair<Long, BlockStoreLocation>> moveIt = plan.toMove().iterator();
    while (moveIt.hasNext()) {
      long id = moveIt.next().getFirst();
      if (!mMetaManager.hasBlockMeta(id)) {
        mLRUCache.remove(id);
        moveIt.remove();
      }
    }
    Iterator<Long> evictIt = plan.toEvict().iterator();
    while (evictIt.hasNext()) {
      long id = evictIt.next();
      if (!mMetaManager.hasBlockMeta(id)) {
        mLRUCache.remove(id);
        evictIt.remove();
      }
    }

    return EvictorUtils.legalCascadingPlan(bytesToBeAvailable, plan, mMetaManager) ? plan : null;
  }

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
