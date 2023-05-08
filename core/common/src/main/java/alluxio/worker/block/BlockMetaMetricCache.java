package alluxio.worker.block;

import java.util.Map;

public class BlockMetaMetricCache {
    long mCapacityBytes;
    long mUsedBytes;
    long mCapacityFree;

    Map<String, Long> mCapacityBytesOnTiers;
    Map<String, Long> mUsedBytesOnTiers;
    Map<String, Long> mFreeBytesOnTiers;
    int mNumberOfBlocks;

    public long getCapacityBytes() {
      return mCapacityBytes;
    }

    public long getUsedBytes() {
      return mUsedBytes;
    }

    public long getCapacityFree() {
      return mCapacityFree;
    }
    public Map<String, Long> getCapacityBytesOnTiers() {
      return mCapacityBytesOnTiers;
    }

    public Map<String, Long> getUsedBytesOnTiers() {
      return mUsedBytesOnTiers;
    }

    public Map<String, Long> getFreeBytesOnTiers() {
      return mFreeBytesOnTiers;
    }
    public int getNumberOfBlocks() {
      return mNumberOfBlocks;
    }
}
