/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is used to evict blocks by LRFU. LRFU evict blocks with minimum CRF, where CRF of a
 * block is the sum of F(t) = pow(1.0 / {@link #mAttenuationFactor}, t * {@link #mStepFactor}).
 * Each access to a block has a F(t) value and t is the time interval since that access to current.
 * As the formula of F(t) shows, when (1.0 / {@link #mStepFactor}) time units passed, F(t) will
 * cut to the (1.0 / {@link #mAttenuationFactor}) of the old value. So {@link #mStepFactor}
 * controls the step and {@link #mAttenuationFactor} controls the attenuation. Actually, LRFU
 * combines LRU and LFU, it evicts blocks with small frequency or large recency. When
 * {@link #mStepFactor} is close to 0, LRFU is close to LFU. Conversely, LRFU is close to LRU
 * when {@link #mStepFactor} is close to 1.
 */
@NotThreadSafe
public final class LRFUEvictor extends AbstractEvictor {
  // Map from block id to the last updated logic time count
  private final Map<Long, Long> mBlockIdToLastUpdateTime = new ConcurrentHashMap<Long, Long>();
  // Map from block id to the CRF value of the block
  private final Map<Long, Double> mBlockIdToCRFValue = new ConcurrentHashMap<Long, Double>();
  // In the range of [0, 1]. Closer to 0, LRFU closer to LFU. Closer to 1, LRFU closer to LRU
  private final double mStepFactor;
  // In the range of [2, INF]
  private final double mAttenuationFactor;
  private final Configuration mConfiguration;

  //logic time count
  private AtomicLong mLogicTimeCount = new AtomicLong(0L);

  /**
   * Creates a new instance of {@link LRFUEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public LRFUEvictor(BlockMetadataManagerView view, Allocator allocator) {
    super(view, allocator);
    mConfiguration = WorkerContext.getConf();
    mStepFactor = mConfiguration
        .getDouble(Constants.WORKER_EVICTOR_LRFU_STEP_FACTOR);
    mAttenuationFactor = mConfiguration
        .getDouble(Constants.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR);
    Preconditions.checkArgument(mStepFactor >= 0.0 && mStepFactor <= 1.0,
        "Step factor should be in the range of [0.0, 1.0]");
    Preconditions.checkArgument(mAttenuationFactor >= 2.0,
        "Attenuation factor should be no less than 2.0");

    // Preloading blocks
    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        for (BlockMeta block : dir.getEvictableBlocks()) {
          mBlockIdToLastUpdateTime.put(block.getBlockId(), 0L);
          mBlockIdToCRFValue.put(block.getBlockId(), 0.0);
        }
      }
    }
  }

  /**
   * Calculates weight of an access, which is the function value of
   * F(t) = pow (1.0 / {@link #mAttenuationFactor}, t * {@link #mStepFactor}).
   *
   * @param logicTimeInterval time interval since that access to current
   * @return Function value of F(t)
   */
  private double calculateAccessWeight(long logicTimeInterval) {
    return Math.pow(1.0 / mAttenuationFactor, logicTimeInterval * mStepFactor);
  }

  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataManagerView view) {
    synchronized (mBlockIdToLastUpdateTime) {
      updateCRFValue();
      mManagerView = view;

      List<BlockTransferInfo> toMove = new ArrayList<BlockTransferInfo>();
      List<Pair<Long, BlockStoreLocation>> toEvict =
          new ArrayList<Pair<Long, BlockStoreLocation>>();
      EvictionPlan plan = new EvictionPlan(toMove, toEvict);
      StorageDirView candidateDir = cascadingEvict(bytesToBeAvailable, location, plan);

      mManagerView.clearBlockMarks();
      if (candidateDir == null) {
        return null;
      }

      return plan;
    }
  }

  @Override
  protected Iterator<Long> getBlockIterator() {
    return Iterators.transform(getSortedCRF().iterator(),
        new Function<Map.Entry<Long, Double>, Long>() {
          @Override
          public Long apply(Entry<Long, Double> input) {
            return input.getKey();
          }
        });
  }

  /**
   * Sorts all blocks in ascending order of CRF.
   *
   * @return the sorted CRF of all blocks
   */
  private List<Map.Entry<Long, Double>> getSortedCRF() {
    List<Map.Entry<Long, Double>> sortedCRF =
        new ArrayList<Map.Entry<Long, Double>>(mBlockIdToCRFValue.entrySet());
    Collections.sort(sortedCRF, new Comparator<Map.Entry<Long, Double>>() {
      @Override
      public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2) {
        return Double.compare(o1.getValue(), o2.getValue());
      }
    });
    return sortedCRF;
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    updateOnAccessAndCommit(blockId);
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    updateOnAccessAndCommit(blockId);
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    updateOnRemoveBlock(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    updateOnRemoveBlock(blockId);
  }

  @Override
  protected void onRemoveBlockFromIterator(long blockId) {
    mBlockIdToLastUpdateTime.remove(blockId);
    mBlockIdToCRFValue.remove(blockId);
  }

  /**
   * This function is used to update CRF of all the blocks according to current logic time. When
   * some block is accessed in some time, only CRF of that block itself will be updated to current
   * time, other blocks who are not accessed recently will only be updated until
   * {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataManagerView)} is called
   * because blocks need to be sorted in the increasing order of CRF. When this function is called,
   * {@link #mBlockIdToLastUpdateTime} and {@link #mBlockIdToCRFValue} need to be locked in case
   * of the changing of values.
   */
  private void updateCRFValue() {
    long currentLogicTime = mLogicTimeCount.get();
    for (Entry<Long, Double> entry : mBlockIdToCRFValue.entrySet()) {
      long blockId = entry.getKey();
      double crfValue = entry.getValue();
      mBlockIdToCRFValue.put(blockId, crfValue
          * calculateAccessWeight(currentLogicTime - mBlockIdToLastUpdateTime.get(blockId)));
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }
  }

  /**
   * Updates {@link #mBlockIdToLastUpdateTime} and {@link #mBlockIdToCRFValue} when block is
   * accessed or committed. Only CRF of the accessed or committed block will be updated, CRF
   * of other blocks will be lazily updated (only when {@link #updateCRFValue()} is called).
   * If the block is updated at the first time, CRF of the block will be set to 1.0, otherwise
   * the CRF of the block will be set to {1.0 + old CRF * F(current time - last update time)}.
   *
   * @param blockId id of the block to be accessed or committed
   */
  private void updateOnAccessAndCommit(long blockId) {
    synchronized (mBlockIdToLastUpdateTime) {
      long currentLogicTime = mLogicTimeCount.incrementAndGet();
      // update CRF value
      // CRF(currentLogicTime)=CRF(lastUpdateTime)*F(currentLogicTime-lastUpdateTime)+F(0)
      if (mBlockIdToCRFValue.containsKey(blockId)) {
        mBlockIdToCRFValue.put(blockId, mBlockIdToCRFValue.get(blockId)
            * calculateAccessWeight(currentLogicTime - mBlockIdToLastUpdateTime.get(blockId))
            + 1.0);
      } else {
        mBlockIdToCRFValue.put(blockId, 1.0);
      }
      // update currentLogicTime to lastUpdateTime
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }
  }

  /**
   * Updates {@link #mBlockIdToLastUpdateTime} and {@link #mBlockIdToCRFValue} when block is
   * removed.
   *
   * @param blockId id of the block to be removed
   */
  private void updateOnRemoveBlock(long blockId) {
    synchronized (mBlockIdToLastUpdateTime) {
      mLogicTimeCount.incrementAndGet();
      mBlockIdToCRFValue.remove(blockId);
      mBlockIdToLastUpdateTime.remove(blockId);
    }
  }
}
