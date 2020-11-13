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

    public double getProbability(StorageDirView dirView) {
      long availableBytes = dirView.getAvailableBytes();
      long capacityBytes = dirView.getCapacityBytes();

      // Rules:
      // 1. If more than CUTOFF left, we use this tier. Prob=100%
      // 2. If the tier is less than block size, ignore this tier. Prob=0%
      // 3. If in the middle, the probability is linear to the space left, the less space the lower.
      if (availableBytes > mCutOffBytes) {
        return 1.0;
      }
      if (availableBytes <= mDefaultBlockSizeBytes) {
        return 0.0;
      }
      double cutoffRatio = (capacityBytes - mCutOffBytes + 0.0) / (capacityBytes - mDefaultBlockSizeBytes);
      // 2 points:
      // Axis X: space usage (commitment)
      // Axis Y: Probability of using this tier
      // (capacity - cutoff, 1.0)
      // (capacity - block, 0.0)
      double x = capacityBytes - availableBytes;
      double k = 1.0 / (mDefaultBlockSizeBytes - mCutOffBytes); // TODO(jiacheng): 0!
      double b = (capacityBytes - mDefaultBlockSizeBytes + 0.0) / (mCutOffBytes - mDefaultBlockSizeBytes);
      double y = k * x + b;
//      LOG.warn("Space usage in tier {} is {}. Probability of staying is {}.", dirView.getParentTierView().getTierViewAlias(), usage, y);
      return y;
    }

    public boolean reviewAllocation(StorageDirView dirView) {
      double chance = getProbability(dirView);
      // Throw a dice
      double dice = RANDOM.nextDouble();
      boolean stay = dice < chance;
      LOG.warn("Dice={}, do we stay: {}", dice, stay);
      return stay;
    }
}
