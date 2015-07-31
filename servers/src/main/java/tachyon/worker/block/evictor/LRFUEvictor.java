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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.exception.NotFoundException;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.StorageTierView;

/**
 * This class is used to evict blocks by LRFU. LRFU combines LRU and LFU, it evicts blocks with
 * small frequency or large recency. Actually, LRFU evicts blocks with minimum CRF. CRF of a block
 * is the sum of F(t) = pow((1.0 / {@link #mAttenuationFactor}, t * {@link #mStepFactor}) and each
 * access to a block has a F(t) value where t is the time interval since that access time to 
 * current. When {@link #mStepFactor} is close to 0, LRFU is close to LFU. Conversely, LRFU is 
 * close to LRU when {@link #mStepFactor} is close to 1
 */
public class LRFUEvictor extends BlockStoreEventListenerBase implements Evictor {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final double DEFAULT_STEP_FACTOR = 0.25;
  private static final double DEFAULT_ATTENUATION_FACTOR = 2.0;

  private BlockMetadataManagerView mManagerView;
  // Map from block id to the last access time of the block
  private Map<Long, Long> mBlockIdToLastAccessTime = new ConcurrentHashMap<Long, Long>();
  // Map from block id to the CRF value of the block
  private Map<Long, Double> mBlockIdToCRFValue = new ConcurrentHashMap<Long, Double>();
  // In the range of [0, 1]. Closer to 0, LRFU closer to LFU. Closer to 1, LRFU closer to LRU
  private final double mStepFactor;
  // In the range of [2, INF]
  private final double mAttenuationFactor;
  private final TachyonConf mTachyonConf;

  //logic time
  private AtomicLong mLogicTimeCount = new AtomicLong(0L);

  public LRFUEvictor(BlockMetadataManagerView view) {
    mManagerView = view;
    mTachyonConf = new TachyonConf();
    mStepFactor = mTachyonConf
        .getDouble(Constants.WORKER_EVICT_STRATEGY_LRFU_STEP_FACTOR, DEFAULT_STEP_FACTOR);
    mAttenuationFactor = mTachyonConf
        .getDouble(Constants.WORKER_EVICT_STRATEGY_LRFU_ATTENUATION_FACTOR, 
            DEFAULT_ATTENUATION_FACTOR);

    // Preloading blocks
    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        for (BlockMeta block : dir.getEvictableBlocks()) {
          mBlockIdToLastAccessTime.put(block.getBlockId(), 0L);
          mBlockIdToCRFValue.put(block.getBlockId(), 0.0);
        }
      }
    }
  }

  /**
   * @return a StorageDirView in the range of location that already has availableBytes larger than
   *         bytesToBeAvailable, otherwise null
   */
  private StorageDirView selectDirWithRequestedSpace(long bytesToBeAvailable,
      BlockStoreLocation location) {
    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
            return dirView;
          }
        }
      }
      return null;
    }

    int tierAlias = location.tierAlias();
    StorageTierView tierView = mManagerView.getTierView(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
          return dirView;
        }
      }
      return null;
    }

    StorageDirView dirView = tierView.getDirView(location.dir());
    return (dirView.getAvailableBytes() >= bytesToBeAvailable) ? dirView : null;
  }

  /**
   * A recursive implementation of cascading LRFU eviction.
   *
   * It will try to free space in next tier view to transfer blocks there, if the next tier view
   * does not have enough free space to hold the blocks, the next next tier view will be tried and
   * so on until the bottom tier is reached, if blocks can not even be transferred to the bottom
   * tier, they will be evicted, otherwise, only blocks to be freed in the bottom tier will be
   * evicted.
   *
   * this method is only used in {@link #freeSpaceWithView}
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location target location to evict blocks from
   * @param plan the plan to be recursively updated, is empty when first called in
   *        {@link #freeSpaceWithView}
   * @param sortedCRF sorted CRF of all blocks in ascending order
   * @return the first StorageDirView in the range of location to evict/move bytes from, or null if
   *         there is no plan
   */
  protected StorageDirView cascadingEvict(long bytesToBeAvailable, BlockStoreLocation location,
      EvictionPlan plan, List<Map.Entry<Long, Double>> sortedCRF) {

    // 1. if bytesToBeAvailable can already be satisfied without eviction, return emtpy plan
    StorageDirView candidateDirView = selectDirWithRequestedSpace(bytesToBeAvailable, location);
    if (candidateDirView != null) {
      return candidateDirView;
    }
    
    // 2. iterate over blocks in increasing order of CRF until we find a dir view that is in
    // the range of location and can satisfy bytesToBeAvailable after evicting its blocks 
    // iterated so far
    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();
    Iterator<Map.Entry<Long, Double>> it = sortedCRF.iterator();
    while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
      Entry<Long, Double> pair = it.next();
      long blockId = pair.getKey();
      double crfValue = pair.getValue();
      try {
        BlockMeta block = mManagerView.getBlockMeta(blockId);
        if (null != block) { // might not present in this view
          if (block.getBlockLocation().belongTo(location)) {
            int tierAlias = block.getParentDir().getParentTier().getTierAlias();
            int dirIndex = block.getParentDir().getDirIndex();
            dirCandidates.add(mManagerView.getTierView(tierAlias).getDirView(dirIndex), blockId,
                block.getBlockSize());
          }
        }
      } catch (NotFoundException nfe) {
        LOG.warn("Remove block {} from LRFU Cache because {}", blockId, nfe);
        it.remove();
      }
    }

    // 3. have no eviction plan
    if (dirCandidates.candidateSize() < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to free space in next tier to move candidate blocks there, evict
    // blocks only when it can not be moved to next tiers
    candidateDirView = dirCandidates.candidateDir();
    List<Long> candidateBlocks = dirCandidates.candidateBlocks();
    List<StorageTierView> tierViewsBelow =
        mManagerView.getTierViewsBelow(candidateDirView.getParentTierView().getTierViewAlias());
    // find a dir in below tiers to transfer blocks there, from top tier to bottom tier
    StorageDirView candidateNextDir = null;
    for (StorageTierView tierView : tierViewsBelow) {
      candidateNextDir =
          cascadingEvict(dirCandidates.candidateSize() - candidateDirView.getAvailableBytes(),
              BlockStoreLocation.anyDirInTier(tierView.getTierViewAlias()), plan, sortedCRF);
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
    return candidateDirView;
  }

  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataManagerView view) {
    synchronized (mBlockIdToLastAccessTime) {
      updateCRFValue();
      mManagerView = view;

      List<Map.Entry<Long, Double>> sortedCRF = getSortedCRF();
      List<Pair<Long, BlockStoreLocation>> toMove =
          new ArrayList<Pair<Long, BlockStoreLocation>>();
      List<Long> toEvict = new ArrayList<Long>();
      EvictionPlan plan = new EvictionPlan(toMove, toEvict);
      StorageDirView candidateDir = cascadingEvict(bytesToBeAvailable, location, plan, sortedCRF);

      if (candidateDir == null) {
        return null;
      }

      return plan;
    }
  }
  
  /**
   * Sort all blocks in ascending order of CRF
   * 
   * @return the sorted CRF of all blocks
   */
  private List<Map.Entry<Long, Double>> getSortedCRF() {
    List<Map.Entry<Long, Double>> sortedCRF = 
        new ArrayList<Map.Entry<Long, Double>>(mBlockIdToCRFValue.entrySet());
    Collections.sort(sortedCRF, new Comparator<Map.Entry<Long, Double>>() {
      public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2) {
        double res = o1.getValue() - o2.getValue();
        if (res < 0) {
          return -1;
        } else if (res > 0) {
          return 1;
        } else {
          return 0;
        }
      }
    });
    return sortedCRF;
  }

  /**
   * Calculate function F(t) = pow (1.0 / {@link #mAttenuationFactor}, t * {@link #mStepFactor})
   * 
   * @param logicTimeInterval time interval since that access to current
   * @return Function value of F(t)
   */
  private double calculateFunction(long logicTimeInterval) {
    return 1.0 * Math.pow(1.0 / mAttenuationFactor, 1.0 * logicTimeInterval * mStepFactor);
  }

  /**
   * This function is used to update CRF of all the blocks according to current logic time. When
   * some block is accessed in some time, only CRF of that block itself will be updated to current
   * time, other blocks who are not accessed recently will only be updated until 
   * {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataManagerView)} is called 
   * because blocks need to be sorted in the increasing order of CRF. When this function is called,
   * {@link #mBlockIdToLastAccessTime} and {@link #mBlockIdToCRFValue} need to be locked in case
   * of the changing of values.
   */
  private void updateCRFValue() {
    long currentLogicTime = mLogicTimeCount.get();
    for (Iterator<Map.Entry<Long, Double>> it = mBlockIdToCRFValue.entrySet().iterator(); it
        .hasNext();) {
      Map.Entry<Long, Double> entry = it.next();
      long blockId = entry.getKey();
      double crfValue = entry.getValue();
      mBlockIdToCRFValue.put(blockId, crfValue
          * calculateFunction(currentLogicTime - mBlockIdToLastAccessTime.get(blockId)));
      mBlockIdToLastAccessTime.put(blockId, currentLogicTime);
    }
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    synchronized (mBlockIdToLastAccessTime) {
      long currentLogicTime = mLogicTimeCount.incrementAndGet();
      // update CRF value
      // CRF(currentLogicTime)=CRF(lastAccessTime)*F(currentLogicTime-lastAccessTime)+F(0) 
      if (mBlockIdToCRFValue.containsKey(blockId)) {
        mBlockIdToCRFValue.put(blockId, mBlockIdToCRFValue.get(blockId)
            * calculateFunction(currentLogicTime - mBlockIdToLastAccessTime.get(blockId))
            + 1.0);
      } else {
        mBlockIdToCRFValue.put(blockId, 1.0);
      }
      // update currentLogicTime to lastAccessTime
      mBlockIdToLastAccessTime.put(blockId, currentLogicTime);
    }
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    synchronized (mBlockIdToLastAccessTime) {
      long currentLogicTime = mLogicTimeCount.incrementAndGet();
      // update CRF value
      // CRF(currentLogicTime)=CRF(lastAccessTime)*F(currentLogicTime-lastAccessTime)+F(0)
      if (mBlockIdToCRFValue.containsKey(blockId)) {
        mBlockIdToCRFValue.put(blockId, mBlockIdToCRFValue.get(blockId)
            * calculateFunction(currentLogicTime - mBlockIdToLastAccessTime.get(blockId))
            + 1.0);
      } else {
        mBlockIdToCRFValue.put(blockId, 1.0);
      }
      // update currentLogicTime to lastAccessTime
      mBlockIdToLastAccessTime.put(blockId, currentLogicTime);
    }
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    synchronized (mBlockIdToLastAccessTime) {
      mLogicTimeCount.incrementAndGet();
      mBlockIdToCRFValue.remove(blockId);
      mBlockIdToLastAccessTime.remove(blockId);
    }
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    synchronized (mBlockIdToLastAccessTime) {
      mLogicTimeCount.incrementAndGet();
      mBlockIdToCRFValue.remove(blockId);
      mBlockIdToLastAccessTime.remove(blockId);
    }
  }
}
