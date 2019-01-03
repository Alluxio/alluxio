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

package alluxio.worker.block.evictor;

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.collections.Pair;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;


@NotThreadSafe
public final class ADSMEvictor extends AbstractEvictor {
  private final StorageTierAssoc mStorageTierAssoc;
  /** Map from block id to the last updated logic time count. */
  private final Map<Long, Long> mBlockIdToLastUpdateTime = new ConcurrentHashMap<>();
  // Map to record System time that block been access or commit lastly
  private final Map<Long, Long> mBlockIdToLastSystemTime = new ConcurrentHashMap<>();
  // Map to record block parallelism
  private final Map<Long, Long> mBlockIdToParallelism = new ConcurrentHashMap<>();
  // Map to record block size(bytes)
  private final Map<Long, Long> mBlockIdToSize = new ConcurrentHashMap<>();
  // Map from block id to the P value of the block
  private final Map<Long, Double> mBlockIdToPValue = new ConcurrentHashMap<>();
  // Map from block id to the SP-factor of the block
  private final Map<Long, Double> mBlockIdToSPFactor = new ConcurrentHashMap<>();
  // Map from block id to Ratio, 升序排列，Ratio小的先淘汰
  private final Map<Long, Double> mBlockIdToRatioValue = new ConcurrentHashMap<>();

  // Euler
  private double Euler = 2.718;
  // probility of re-access is equal to p(X+x)= S(X)* e^(-a * x) or +1
  private double lambda = 0.35;
  private double AFactor = 0.4;

  // max size of block, 128MB
  private double mMaxSize = 128 * 1024 * 1024;

  // max parallelism of block, 30
  private double mMaxParallelism = 10;
  /** Logic time count. */
  private AtomicLong mLogicTimeCount = new AtomicLong(0L);
  private List<Double> lambdaList = Arrays.asList(0.35, 0.35, 0.35, 0.35, 0.35, 0.4, 0.4, 0.4, 0.4, 0.4, 0.45, 0.45, 0.45, 0.45, 0.45);
  // Value space of AFactor, the bigger AFactor is, the closer ADSM gets to LRU policy
  private List<Double> AList = Arrays.asList(0.4, 0.3, 0.2, 0.1, 10e-10, 0.4, 0.3, 0.2, 0.1, 10e-10, 0.4, 0.3, 0.2, 0.1, 10e-10);
  // eventNumOfRound is the num of event each phase
  private int MaxPhaseNumOfRound = 15;
  // each phase consists of 10 events, each phase evaluate each variable, and each event indicite an action to
  // move a block other tier to the top tier
  private int MaxEventNumOfPhase = 10;
  private int phaseNumOfRound = 0;
  private int eventNumOfPhase = 0;

  private double accessSPFactor = 0;
  private double moveSPFactor = 0;
  private double shouldMoveSPFactor = 0;
  private double evictSPFactor = 0;
  private double promoteSPFactor = 0;

  private double accessSize = 0;
  private double moveSize = 0;
  private double shouldMoveSize = 0;
  private double promoteSize = 0;

  private double SPHR = 0;
  private double SPIR = 0;
  private double BHR = 0;
  private double BIR = 0;
  private final Map<Integer,Double> phaseToSPHR = new ConcurrentHashMap<>();
  private final Map<Integer,Double> phaseToSPIR = new ConcurrentHashMap<>();
  private final Map<Integer,Double> phaseToBHR = new ConcurrentHashMap<>();
  private final Map<Integer,Double> phaseToBIR = new ConcurrentHashMap<>();
  private boolean AutoTune = false;
  private Long lastTuningLogicTime = 0L;

//  private FileWriter writer = null;
  /**
   * Creates a new instance of {@link LRFUEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public ADSMEvictor(BlockMetadataManagerView view, Allocator allocator) {
    super(view, allocator);
    mStorageTierAssoc = new WorkerStorageTierAssoc();

    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        for (BlockMeta block : dir.getEvictableBlocks()) {
          mBlockIdToLastUpdateTime.put(block.getBlockId(), 0L);
          mBlockIdToParallelism.put(block.getBlockId(), 1L);
          mBlockIdToPValue.put(block.getBlockId(), 0.0);
          mBlockIdToSize.put(block.getBlockId(), block.getBlockSize());
          mBlockIdToSPFactor.put(block.getBlockId(), 0.0);
          mBlockIdToRatioValue.put(block.getBlockId(),0.0);
        }
      }
    }
  }

  /**
   * Calculates weight of an access
   * @param logicTimeInterval time interval since that access to current
   * @return Function value of F(t)
   */
  /*
  private double calculateAccessWeight(long logicTimeInterval) {
    return Math.pow(1.0 / mAttenuationFactor, logicTimeInterval * mStepFactor);
  }
  */

  private double calculateAccessWeight(long logicTimeInterval) {
    return Math.pow(Euler, (-1) * AFactor * logicTimeInterval);
  }

  @Nullable
  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
                                        BlockMetadataManagerView view, Mode mode) {
    synchronized (mBlockIdToLastUpdateTime) {
      updatePValueWithoutAccess();
      updateRatioValue();
      mManagerView = view;
      List<BlockTransferInfo> toMove = new ArrayList<>();
      List<Pair<Long, BlockStoreLocation>> toEvict = new ArrayList<>();
      EvictionPlan plan = new EvictionPlan(toMove, toEvict);
      StorageDirView candidateDir = cascadingEvict(bytesToBeAvailable, location, plan, mode);
      mManagerView.clearBlockMarks();
      if (candidateDir == null) {
        return null;
      }
      return plan;
    }
  }

  @Override
  protected Iterator<Long> getBlockIterator() {
    return Iterators.transform(getSortedRatio().iterator(),
            new Function<Map.Entry<Long, Double>, Long>() {
              @Override
              //Entry<Long blockId, Double CRFValue>
              public Long apply(Entry<Long, Double> input) {
                return input.getKey();
              }
            });
  }


  /**
   * Sorts all blocks in ascending order of Ratio.
   *
   * @return the sorted Ratio of all blocks
   */
  private List<Map.Entry<Long, Double>> getSortedRatio() {
    List<Map.Entry<Long, Double>> sortedRatio = new ArrayList<>(mBlockIdToRatioValue.entrySet());
    Collections.sort(sortedRatio, new Comparator<Map.Entry<Long, Double>>() {
      // default ascending order
      @Override
      public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2) {
        return Double.compare(o1.getValue(), o2.getValue());
      }
    });
    return sortedRatio;
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    if(mBlockIdToSPFactor.containsKey(blockId)){
      accessSPFactor += mBlockIdToSPFactor.get(blockId);
    }
    if(mBlockIdToSize.containsKey(blockId)){
      accessSize += mBlockIdToSize.get(blockId);
    }
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
    mBlockIdToSPFactor.remove(blockId);
    mBlockIdToPValue.remove(blockId);
    mBlockIdToRatioValue.remove(blockId);
    mBlockIdToLastSystemTime.remove(blockId);
    mBlockIdToParallelism.remove(blockId);
    mBlockIdToSize.remove(blockId);
  }

  @Override
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
                                  BlockStoreLocation newLocation) {
    updateOnMove(blockId, oldLocation, newLocation);
  }

  @Override
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
                                  BlockStoreLocation newLocation) {
    updateOnMove(blockId, oldLocation, newLocation);
  }


  public void updateOnMove(long blockId, BlockStoreLocation oldLocation,
                                  BlockStoreLocation newLocation) {
    synchronized (mBlockIdToLastUpdateTime) {
      mLogicTimeCount.incrementAndGet();
      if(lastTuningLogicTime == 0 || (mLogicTimeCount.get() - lastTuningLogicTime) > 500){
        AutoTune = true;
        lastTuningLogicTime = mLogicTimeCount.get();
        AFactor = AList.get(0);
        lambda  = lambdaList.get(0);
        accessSPFactor = 0;
        accessSize = 0;
        shouldMoveSize = 0;
        shouldMoveSPFactor = 0;
        evictSPFactor = 0;
      }
      if(AutoTune) {
        int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
        int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());

        if(newTierOrdinal == 1 && oldTierOrdinal ==0){
          // an evict action
          evictSPFactor += mBlockIdToSPFactor.get(blockId);
        }

        if (newTierOrdinal == 0 && oldTierOrdinal == 1) {
          if(mBlockIdToSPFactor.containsKey(blockId)){
            moveSPFactor += mBlockIdToSPFactor.get(blockId);
            promoteSPFactor = mBlockIdToSPFactor.get(blockId);
            if(evictSPFactor !=0) {
              // the first promotion after evict
              if (promoteSPFactor > evictSPFactor) {
                shouldMoveSPFactor += promoteSPFactor;
              }
            }
          }
          if(mBlockIdToSize.containsKey(blockId)){
            moveSize += mBlockIdToSize.get(blockId);
            promoteSize = mBlockIdToSize.get(blockId);
            if(evictSPFactor != 0) {
              if (promoteSPFactor > evictSPFactor) {
                shouldMoveSize += promoteSize;
              }
              evictSPFactor = 0;
            }
          }

          eventNumOfPhase++;
          if (eventNumOfPhase == MaxEventNumOfPhase) {
            // complete a phase
            eventNumOfPhase = 0;
            SPHR = (accessSPFactor - moveSPFactor) / accessSPFactor;
            SPIR = shouldMoveSPFactor / accessSPFactor;
            BHR = (accessSize - moveSize) / accessSize;
            BIR = shouldMoveSize / accessSize;
            if (!phaseToSPHR.containsKey(phaseNumOfRound)) {
              phaseToSPHR.put(phaseNumOfRound, SPHR);
              phaseToSPIR.put(phaseNumOfRound, SPIR);
              phaseToBHR.put(phaseNumOfRound, BHR);
              phaseToBIR.put(phaseNumOfRound, BIR);
            } else {
              SPHR = phaseToSPHR.get(phaseNumOfRound) * 0.7 + SPHR * 0.3;
              SPIR = phaseToSPIR.get(phaseNumOfRound) * 0.7 + SPIR * 0.3;
              BHR = phaseToBHR.get(phaseNumOfRound) * 0.7 + BHR * 0.3;
              BIR = phaseToBIR.get(phaseNumOfRound) * 0.7 + BIR * 0.3;
              phaseToSPHR.put(phaseNumOfRound, SPHR);
              phaseToSPIR.put(phaseNumOfRound, SPIR);
              phaseToBHR.put(phaseNumOfRound, BHR);
              phaseToBIR.put(phaseNumOfRound, BIR);
            }
            phaseNumOfRound++;
            if (phaseNumOfRound == MaxPhaseNumOfRound) {
              // complete a round
              phaseNumOfRound = 0;
              updateParameter();
              lastTuningLogicTime = mLogicTimeCount.get();
              AutoTune = false;
            } else {
              AFactor = AList.get(phaseNumOfRound);
              lambda = lambdaList.get(phaseNumOfRound);
            }
            accessSPFactor = 0;
            moveSPFactor = 0;
            accessSize = 0;
            moveSize =0;
            shouldMoveSize=0;
            shouldMoveSPFactor=0;
          }

        }

      }
    }
  }

  void updateParameter(){
    double optimalScore =0;
    double score = 0;
    int phase =0;
    for(int i=0;i<15;i++){
      score = 0.35 * phaseToBHR.get(i) - 0.4 * phaseToBIR.get(i) + 0.3 * phaseToSPHR.get(i) - 0.24 * phaseToSPIR.get(i);
      if(score > optimalScore){
        optimalScore = score;
        phase = i;
      }
    }
    lambda = lambdaList.get(phase);
    AFactor = AList.get(phase);
  }

  private void updatePValueWithoutAccess() {
    long currentLogicTime = mLogicTimeCount.get();
    for (Entry<Long, Double> entry : mBlockIdToPValue.entrySet()) {
      long blockId = entry.getKey();
      double PValue = entry.getValue();
      mBlockIdToPValue.put(blockId, PValue
              * calculateAccessWeight(currentLogicTime - mBlockIdToLastUpdateTime.get(blockId)));
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }
  }

  private void updatePValue(Long blockId) {
    long currentLogicTime = mLogicTimeCount.get();
    if(mBlockIdToPValue.containsKey(blockId)){
      double PValue = mBlockIdToPValue.get(blockId);
      mBlockIdToPValue.put(blockId, PValue
              * calculateAccessWeight(currentLogicTime - mBlockIdToLastUpdateTime.get(blockId)) + 1.0);
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }else{
      mBlockIdToPValue.put(blockId,1.0);
    }
  }
  private void updateRatioValue() {
    for(Entry<Long,Double> entry : mBlockIdToSPFactor.entrySet()) {
      long blockId = entry.getKey();
      double SPFactor;
      double PValue;
      double SizeValue;
      double RatioValue;

      SPFactor = mBlockIdToSPFactor.get(blockId);

      if(mBlockIdToPValue.containsKey(blockId)) {
        PValue = mBlockIdToPValue.get(blockId);
      } else {
        PValue = 0.5;
      }

      if(mBlockIdToSize.containsKey(blockId)) {
        SizeValue = mBlockIdToSize.get(blockId) / mMaxSize;
      } else {
        SizeValue = 1;
      }

      RatioValue = (SPFactor * lambda +  PValue * (1-lambda)) / SizeValue ;
      mBlockIdToRatioValue.put(blockId, RatioValue);
    }
  }

  private void updateOnAccessAndCommit(long blockId) {
    synchronized (mBlockIdToLastUpdateTime) {
      long currentLogicTime = mLogicTimeCount.incrementAndGet();
      //get the system time when a block has been accessed or committed
      long currentSystemTime = System.currentTimeMillis();
      long parallel = 1;
      //if mBlockIdToLastSystemTime contain the blockid, then update that, if not add it
      mBlockIdToLastSystemTime.put(blockId, currentSystemTime);


      for (Entry<Long, Long> entry : mBlockIdToLastSystemTime.entrySet()) {
        if (currentSystemTime - 5000 <= entry.getValue()
                && entry.getValue() <= currentSystemTime) {
          parallel++;
        }
      }
      mBlockIdToParallelism.put(blockId, parallel);
      // if mBlockIdToSize contain the blockId, then do nothing, else get the block size
      if (mBlockIdToSize.containsKey(blockId)) {
        // do nothing
      } else {
        for (StorageTierView tier : mManagerView.getTierViews()) {
          for (StorageDirView dir : tier.getDirViews()) {
            for (BlockMeta block : dir.getEvictableBlocks()) {
              if (block.getBlockId() == blockId) {
                mBlockIdToSize.put(blockId, block.getBlockSize());
              }
            }
          }
        }
      }
      // update  SP-factor
      // SP-factor = e ^ {-1 * (MaxSize / size) * [ (para - MaxPara) / MaxPara)]^2}
      mBlockIdToSPFactor.put(blockId, calculateSPFactor(blockId));

      // update PValue
      updatePValue(blockId);
      // update currentLogicTime to lastUpdateTime
      mBlockIdToLastUpdateTime.put(blockId, currentLogicTime);
    }
  }

  private double calculateSPFactor(long blockId){
    // SP-factor = e^{-(MaxSize / size)*[(Paral - MaxParal)/MaxParal]^2}
    // so if the size is more large or the paral is more close to MaxParal, the SP-factor is greater
    double sizeSector;
    double parallelismSector;
    long size;
    long parallelism;
    if (mBlockIdToSize.containsKey(blockId)) {
      size = mBlockIdToSize.get(blockId);
      sizeSector = mMaxSize / size;
    } else {
      sizeSector = 1;
    }

    if (mBlockIdToParallelism.containsKey(blockId)) {
      parallelism = mBlockIdToParallelism.get(blockId);
      parallelismSector = Math.pow((mMaxParallelism - parallelism) / mMaxParallelism, 2);
    } else {
      parallelismSector = 0;
    }


    return Math.pow(Euler,(sizeSector * parallelismSector) * (-1));
  }

  /**
   * Updates {@link #mBlockIdToLastUpdateTime}  when block is
   * removed.
   *
   * @param blockId id of the block to be removed
   */
  private void updateOnRemoveBlock(long blockId) {
    synchronized (mBlockIdToLastUpdateTime) {
      mLogicTimeCount.incrementAndGet();
      mBlockIdToSPFactor.remove(blockId);
      mBlockIdToPValue.remove(blockId);
      mBlockIdToRatioValue.remove(blockId);
      mBlockIdToLastUpdateTime.remove(blockId);
      mBlockIdToParallelism.remove(blockId);
      mBlockIdToSize.remove(blockId);
    }
  }
}
