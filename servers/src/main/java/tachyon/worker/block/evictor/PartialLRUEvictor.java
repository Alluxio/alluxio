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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This class is used to evict old blocks in certain StorageDir by LRU. The main difference
 * between PartialLRU and LRU is that LRU choose old blocks among several StorageDirs
 * until one StorageDir satisfies the request space, but PartialLRU select one StorageDir
 * first and evict old blocks in certain StorageDir by LRU
 */
public class PartialLRUEvictor extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMetadataManager mMeta;

  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
  private static final boolean UNUSED_MAP_VALUE = true;

  /**
   * access-ordered {@link java.util.LinkedHashMap} from blockId to {@link #UNUSED_MAP_VALUE}(just a
   * placeholder to occupy the value), acts as a LRU double linked list where most recently accessed
   * element is put at the tail while least recently accessed element is put at the head.
   */
  private Map<Long, Boolean> mPartialLRUCache = Collections
      .synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  public PartialLRUEvictor(BlockMetadataManager meta) {
    mMeta = Preconditions.checkNotNull(meta);

    // preload existing blocks loaded by StorageDir to Evictor
    for (StorageTier tier : mMeta.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        for (long blockId : dir.getBlockIds()) {
          mPartialLRUCache.put(blockId, UNUSED_MAP_VALUE);
        }
      }
    }
  }

  @Override
  public EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();

    Pair<StorageDir, List<Long>> dirCandidate = getDirCandidate(availableBytes, location);

    if (dirCandidate == null) {
      LOG.error("Failed to freeSpace: No StorageDir has enough capacity of {} bytes",
          availableBytes);
      return null;
    }

    StorageDir selectedDir = dirCandidate.getFirst();
    List<Long> victimBlocks = dirCandidate.getSecond();
    Collections.reverse(victimBlocks);
    if (selectedDir.getAvailableBytes() >= availableBytes) {
      return new EvictionPlan(toMove, toEvict);
    }

    Map<StorageDir, Long> pendingBytesInDir = new HashMap<StorageDir, Long>();
    for (long blockId : victimBlocks) {
      StorageTier fromTier = mMeta.getBlockMeta(blockId).getParentDir().getParentTier();
      List<StorageTier> toTiers = mMeta.getTiersBelow(fromTier.getTierAlias());
      StorageDir toDir =
          selectDirToMoveBlock(mMeta.getBlockMeta(blockId), toTiers, pendingBytesInDir);
      if (toDir == null) {
        // Not possible to Move
        toEvict.add(blockId);
      } else {
        StorageTier toTier = toDir.getParentTier();
        toMove.add(new Pair<Long, BlockStoreLocation>(blockId, new BlockStoreLocation(toTier
            .getTierAlias(), toTier.getTierLevel(), toDir.getDirIndex())));
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir)
              + mMeta.getBlockMeta(blockId).getBlockSize());
        } else {
          pendingBytesInDir.put(toDir, mMeta.getBlockMeta(blockId).getBlockSize());
        }
      }
    }
    return new EvictionPlan(toMove, toEvict);
  }

  /**
   * Get StorageDir with max free space and the blocks to be evicted
   * 
   * @param availableBytes space size to be requested
   * @param location the location that the space will be allocated in
   * @return the pair of StorageDir to be selected and the blocks to be evicted
   * @throws IOException
   */
  private Pair<StorageDir, List<Long>> getDirCandidate(long availableBytes,
      BlockStoreLocation location) throws IOException {
    Set<StorageDir> ignoreList = new HashSet<StorageDir>();
    StorageDir selectedDir = getDirWithMaxFreeSpace(availableBytes, location, ignoreList);

    while (selectedDir != null) {
      long freedBytes = 0;
      List<Long> victimBlocks = new ArrayList<Long>();
      Iterator<Map.Entry<Long, Boolean>> it = mPartialLRUCache.entrySet().iterator();

      while (it.hasNext() && freedBytes + selectedDir.getAvailableBytes() < availableBytes) {
        long blockId = it.next().getKey();

        try {
          BlockMeta meta = mMeta.getBlockMeta(blockId);
          if (meta.getParentDir().getStorageDirId() == selectedDir.getStorageDirId()) {
            freedBytes += meta.getBlockSize();
            victimBlocks.add(blockId);
          }
        } catch (IOException ioe) {
          LOG.warn("Remove block %d from PartialLRU Cache because %s", blockId, ioe);
          it.remove();
        }
      }

      if (freedBytes + selectedDir.getAvailableBytes() < availableBytes) {
        ignoreList.add(selectedDir);
        selectedDir = getDirWithMaxFreeSpace(availableBytes, location, ignoreList);
      } else {
        return new Pair<StorageDir, List<Long>>(selectedDir, victimBlocks);
      }
    }
    return null;
  }

  /**
   * Get StorageDir with max free space. IgnoreList here is considering when the first selected
   * StorageDir with max free space fails to allocate space, then the StorageDir with second
   * max free space will be selected. IgnoreList here just keeps it more safe.
   * 
   * @param availableBytes space size to be requested
   * @param location location that the space will be allocated in
   * @param ignoreList StorageDirs that have been ignored
   * @return the StorageDir selected
   * @throws IOException
   */
  private StorageDir getDirWithMaxFreeSpace(long availableBytes, BlockStoreLocation location,
      Set<StorageDir> ignoreList) throws IOException {
    long maxFreeSize = -1;
    StorageDir selectedDir = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mMeta.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          if (ignoreList.contains(dir)) {
            continue;
          }
          if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
              && dir.getAvailableBytes() > maxFreeSize) {
            selectedDir = dir;
            maxFreeSize = dir.getAvailableBytes();
          }
        }
      }
    } else {
      int tierAlias = location.tierAlias();
      StorageTier tier = mMeta.getTier(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        // Loop over all dirs in the given tier
        for (StorageDir dir : tier.getStorageDirs()) {
          if (ignoreList.contains(dir)) {
            continue;
          }
          if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
              && dir.getAvailableBytes() > maxFreeSize) {
            selectedDir = dir;
            maxFreeSize = dir.getAvailableBytes();
          }
        }
      } else {
        int dirIndex = location.dir();
        StorageDir dir = tier.getDir(dirIndex);
        if (!ignoreList.contains(dir)
            && dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
            && dir.getAvailableBytes() > maxFreeSize) {
          selectedDir = dir;
          maxFreeSize = dir.getAvailableBytes();
        }
      }
    }
    return selectedDir;
  }

  /**
   * Select StorageDir for current block to move to
   * 
   * @param block block to be moved
   * @param toTiers StorageTier below the current StorageTier
   * @param pendingBytesInDir bytes shouldn't be evicted in StorageDirs
   * @return the StorageDir selected
   */
  private StorageDir selectDirToMoveBlock(BlockMeta block, List<StorageTier> toTiers,
      Map<StorageDir, Long> pendingBytesInDir) {
    for (StorageTier toTier : toTiers) {
      for (StorageDir toDir : toTier.getStorageDirs()) {
        long pendingBytes = 0;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if (toDir.getAvailableBytes() - pendingBytes >= block.getBlockSize()) {
          return toDir;
        }
      }
    }
    return null;
  }

  /**
   * Thread safe
   */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    mPartialLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    mPartialLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    mPartialLRUCache.remove(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    mPartialLRUCache.remove(blockId);
  }
}
