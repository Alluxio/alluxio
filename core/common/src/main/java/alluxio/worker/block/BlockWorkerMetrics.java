package alluxio.worker.block;

import alluxio.StorageTierAssoc;
import alluxio.util.CommonUtils;

import java.util.Map;

/**
 * The BlockMetaMetricCache cache the metric data of the BlockMeta from the BlockWorker.
 *
 * So the BlockWorker can pass this MetricCache to registerGauge instead of let registerGauge
 * copy a whole BlockMeta everytime updating the metrics.
 */
public class BlockWorkerMetrics {
  final long mLastUpdateTimeStamp;
  final long mCapacityBytes;
  final long mUsedBytes;
  final long mCapacityFree;

  final Map<String, Long> mCapacityBytesOnTiers;
  final Map<String, Long> mUsedBytesOnTiers;
  final Map<String, Long> mFreeBytesOnTiers;
  final int mNumberOfBlocks;

  public BlockWorkerMetrics(BlockStoreMeta meta, StorageTierAssoc s) {
    mLastUpdateTimeStamp = CommonUtils.getCurrentMs();
    mCapacityBytes = meta.getCapacityBytes();
    mUsedBytes = meta.getUsedBytes();
    mCapacityFree = mCapacityBytes - mUsedBytes;
    mCapacityBytesOnTiers = meta.getCapacityBytesOnTiers();
    mUsedBytesOnTiers = meta.getCapacityBytesOnTiers();
    Map<String, Long> tmpMap = meta.getCapacityBytesOnTiers();
    for (int i = 0; i < s.size(); i++) {
      String tier = s.getAlias(i);
      tmpMap.replace(tier, mCapacityBytesOnTiers
          .getOrDefault(tier, 0L)
          - mUsedBytesOnTiers.getOrDefault(tier, 0L));
    }
    mFreeBytesOnTiers = tmpMap;
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

  public static BlockWorkerMetrics from(BlockStoreMeta meta, StorageTierAssoc s) {
    return new BlockWorkerMetrics(meta, s);
  }
}
