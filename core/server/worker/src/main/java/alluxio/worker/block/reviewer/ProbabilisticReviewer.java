package alluxio.worker.block.reviewer;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.meta.StorageDirView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Reviews a block allocation, and probably reject a bad block allocation.
 * The more full a StorageDir, the higher the chance that this reviewer decides
 * not to use it for a new block.
 * */
public class ProbabilisticReviewer implements Reviewer {
    private static final Logger LOG = LoggerFactory.getLogger(ProbabilisticReviewer.class);

    private static final Random RANDOM = new Random();

    private final long mDefaultBlockSizeBytes;
    private final long mCutOffBytes;
    private final InstancedConfiguration mConf;

    public ProbabilisticReviewer() {
      mConf = ServerConfiguration.global();
      mDefaultBlockSizeBytes = mConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
      mCutOffBytes = mConf.getBytes(PropertyKey.WORKER_TIER_CUTOFF_BYTES);
    }

    public boolean reviewAllocation(StorageDirView dirView) {
      long availableBytes = dirView.getAvailableBytes();
      long capacityBytes = dirView.getCapacityBytes();

      // Rules:
      // 1. If more than CUTOFF left, we use this tier. Prob=100%
      // 2. If the tier is less than block size, ignore this tier. Prob=0%
      // 3. If in the middle, the probability is linear to the space left, the less space the lower.
      if (availableBytes > mCutOffBytes) {
        return true;
      }
      double usage = (capacityBytes - availableBytes + 0.01) / (capacityBytes - mDefaultBlockSizeBytes);
      if (usage >= 1.0) {
        return false;
      }
      double cutoffRatio = (capacityBytes - mCutOffBytes + 0.01) / (capacityBytes - mDefaultBlockSizeBytes);
      // 2 points:
      // Axis X: space usage (commitment)
      // Axis Y: Probability of using this tier
      // (1.0 - cutoffRatio, 1.0)
      // (1.0, 0)
      double k = -1.0 / cutoffRatio; // What if CUTOFF == 0?
      double b = 1.0 / cutoffRatio;
      double y = k * usage + b;
      LOG.warn("Space usage in tier {} is {}. Probability of staying is {}.", dirView.getParentTierView().getTierViewAlias(), usage, y);
      // Throw a dice
      double dice = RANDOM.nextDouble();
      boolean stay = dice < y;
      LOG.warn("Dice={}, do we stay: {}", dice, stay);
      return stay;
    }
}
