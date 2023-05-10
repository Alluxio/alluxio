package alluxio.worker.block;

import alluxio.StorageTierAssoc;
import alluxio.util.CommonUtils;
import alluxio.worker.block.meta.BlockMeta;

import java.util.Map;

/**
 * The BlockMetaMetricCache cache the metric data of the BlockMeta from the BlockWorker.
 *
 * So the BlockWorker can pass this MetricCache to registerGauge instead of let registerGauge
 * copy a whole BlockMeta everytime updating the metrics.
 */
public class BlockWorkerMetrics {
  BlockWorker mBlockWorker;
  long mLastUpdateTimeStamp;
  long mCapacityBytes;
  long mUsedBytes;
  long mCapacityFree;

  Map<String, Long> mCapacityBytesOnTiers;
  Map<String, Long> mUsedBytesOnTiers;
  Map<String, Long> mFreeBytesOnTiers;
  int mNumberOfBlocks;

  public BlockWorkerMetrics(BlockStoreMeta meta, StorageTierAssoc s) {
    mLastUpdateTimeStamp = CommonUtils.getCurrentMs();
    mCapacityBytes = meta.getCapacityBytes();
    mUsedBytes = meta.getUsedBytes();
    mCapacityFree = mCapacityBytes - mUsedBytes;
    mCapacityBytesOnTiers = meta.getCapacityBytesOnTiers();
    mUsedBytesOnTiers = meta.getCapacityBytesOnTiers();
    for (int i = 0; i < s.size(); i++) {
      String tier = s.getAlias(i);
      mFreeBytesOnTiers.replace(tier, mCapacityBytesOnTiers
          .getOrDefault(tier, 0L)
          - mUsedBytesOnTiers.getOrDefault(tier, 0L));
    }
    mNumberOfBlocks = meta.getNumberOfBlocks();
  }

  /**
   * @return last update time
   */
  public long getLastUpdateTimeStamp() {
    return mLastUpdateTimeStamp;
  }

  /**
   * @return the capacityBytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the usedBytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the freeCapacityBytes
   */
  public long getCapacityFree() {
    return mCapacityFree;
  }

  /**
   * @return the tierCapacityBytes map
   */
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  /**
   * @return the tierUsedBytes map
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  /**
   * @return the tierFreeBytes map
   */
  public Map<String, Long> getFreeBytesOnTiers() {
    return mFreeBytesOnTiers;
  }

  /**
   * @return the numberOfBlocks
   */
  public int getNumberOfBlocks() {
    return mNumberOfBlocks;
  }

  public void update(StorageTierAssoc s) {
    BlockStoreMeta meta = mBlockWorker.getBlockStore().getBlockStoreMetaFull();
    mLastUpdateTimeStamp = CommonUtils.getCurrentMs();
    mCapacityBytes = meta.getCapacityBytes();
    mUsedBytes = meta.getUsedBytes();
    mCapacityFree = mCapacityBytes - mUsedBytes;
    mCapacityBytesOnTiers = meta.getCapacityBytesOnTiers();
    mUsedBytesOnTiers = meta.getCapacityBytesOnTiers();
    for (int i = 0; i < s.size(); i++) {
      String tier = s.getAlias(i);
      mFreeBytesOnTiers.replace(tier, mCapacityBytesOnTiers
          .getOrDefault(tier, 0L)
          - mUsedBytesOnTiers.getOrDefault(tier, 0L));
    }
    mNumberOfBlocks = meta.getNumberOfBlocks();
  }

  public static BlockWorkerMetrics from(BlockStoreMeta meta, StorageTierAssoc s) {
    return new BlockWorkerMetrics(meta, s);
  }
}
