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
  private final long mCapacityBytes;
  private final long mUsedBytes;
  private final long mCapacityFree;

  private final Map<String, Long> mCapacityBytesOnTiers;
  private final Map<String, Long> mUsedBytesOnTiers;
  private final Map<String, Long> mFreeBytesOnTiers;
  private final int mNumberOfBlocks;

  public BlockWorkerMetrics(long capacityBytes, long usedBytes, long capacityFree, Map<String, Long> capacityBytesOnTiers, Map<String, Long> usedBytesOnTiers, Map<String, Long> freeBytesOnTiers, int numberOfBlocks) {
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
    mCapacityFree = capacityFree;
    mCapacityBytesOnTiers = capacityBytesOnTiers;
    mUsedBytesOnTiers = usedBytesOnTiers;
    mFreeBytesOnTiers = freeBytesOnTiers;
    mNumberOfBlocks = numberOfBlocks;
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
    long mCapacityBytes = meta.getCapacityBytes();
    long mUsedBytes = meta.getUsedBytes();
    long mCapacityFree = mCapacityBytes - mUsedBytes;
    Map<String, Long> mCapacityBytesOnTiers = meta.getCapacityBytesOnTiers();
    Map<String, Long> mUsedBytesOnTiers = meta.getCapacityBytesOnTiers();
    Map<String, Long> mFreeBytesOnTiers = meta.getCapacityBytesOnTiers();
    for (int i = 0; i < s.size(); i++) {
      String tier = s.getAlias(i);
      mFreeBytesOnTiers.replace(tier, mCapacityBytesOnTiers
          .getOrDefault(tier, 0L)
          - mUsedBytesOnTiers.getOrDefault(tier, 0L));
    }
    int mNumberOfBlocks = meta.getNumberOfBlocks();
    return new BlockWorkerMetrics(mCapacityBytes, mUsedBytes, mCapacityFree, mCapacityBytesOnTiers, mUsedBytesOnTiers, mFreeBytesOnTiers, mNumberOfBlocks);
  }
}
