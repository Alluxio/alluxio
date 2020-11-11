package alluxio.worker.block.reviewer;

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

    // TODO(jiacheng): Make this a property
    private static final double CUTOFF = 0.1;
    private static final Random RANDOM = new Random();

    public ProbabilisticReviewer() {}

    public boolean reviewAllocation(StorageDirView dirView) {
      long availableBytes = dirView.getAvailableBytes();
      long capacityBytes = dirView.getCapacityBytes();
      long blockSize = ServerConfiguration.global().getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
      long cutoffSize = (long) Math.floor(CUTOFF * capacityBytes);
      // Rules:
      // 1. If more than CUTOFF left, we use this tier. Prob=100%
      // 2. If less that block size left, we ignore this tier. Prob=0%
      // 3. If in the middle, the probability is linear to the space left, the less space the lower.
      if (availableBytes > cutoffSize) {
        return true;
      }
      double usage = (capacityBytes - availableBytes + 0.01) / (capacityBytes - blockSize);
      if (usage >= 1.0) {
        return false;
      }
      // 2 points:
      // Axis X: space usage (commitment)
      // Axis Y: Probability of using this tier
      // (1.0 - CUTOFF, 1.0)
      // (1.0, 0)
      double k = -1.0 / CUTOFF; // What if CUTOFF == 0?
      double b = 1.0 / CUTOFF;
      double y = k * usage + b;
      LOG.warn("Space usage in tier {} is {}. Probability of staying is {}.", dirView.getParentTierView().getTierViewAlias(), usage, y);
      // Throw a dice
      double dice = RANDOM.nextDouble();
      boolean stay = dice < y;
      LOG.warn("Dice={}, do we stay: {}", dice, stay);
      return stay;
    }
}
