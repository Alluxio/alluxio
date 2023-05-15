package alluxio.worker.block;

import alluxio.StorageTierAssoc;

import java.util.HashMap;
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

  /**
   * construct func of BlockWorkerMetrics.
   * @param capacityBytes
   * @param usedBytes
   * @param capacityFree
   * @param capacityBytesOnTiers
   * @param usedBytesOnTiers
   * @param freeBytesOnTiers
   * @param numberOfBlocks
   */
  public BlockWorkerMetrics(long capacityBytes, long usedBytes, long capacityFree,
                            Map<String, Long> capacityBytesOnTiers,
                            Map<String, Long> usedBytesOnTiers,
                            Map<String, Long> freeBytesOnTiers, int numberOfBlocks) {
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

  /**
   * return a new BlockWorkerMetrics form a new BlockStoreMeta.
   * @param meta new BlockStoreMeta
   * @param s the StorageTierAssoc, can't import here so pass it as param
   * @return a new BlockWorkerMetrics
   */
  public static BlockWorkerMetrics from(BlockStoreMeta meta, StorageTierAssoc s) {
    long capacityBytes = meta.getCapacityBytes();
    long usedBytes = meta.getUsedBytes();
    long capacityFree = capacityBytes - usedBytes;
    Map<String, Long> capacityBytesOnTiers = meta.getCapacityBytesOnTiers();
    Map<String, Long> usedBytesOnTiers = meta.getCapacityBytesOnTiers();
    // freeBytesOnTiers is recalculated
    Map<String, Long> freeBytesOnTiers = meta.getCapacityBytesOnTiers();
    for (int i = 0; i < s.size(); i++) {
      String tier = s.getAlias(i);
      freeBytesOnTiers.replace(tier, capacityBytesOnTiers
          .getOrDefault(tier, 0L)
          - usedBytesOnTiers.getOrDefault(tier, 0L));
    }
    int numberOfBlocks = meta.getNumberOfBlocks();
    return new BlockWorkerMetrics(capacityBytes, usedBytes, capacityFree,
        capacityBytesOnTiers, usedBytesOnTiers, freeBytesOnTiers, numberOfBlocks);
  }
}
