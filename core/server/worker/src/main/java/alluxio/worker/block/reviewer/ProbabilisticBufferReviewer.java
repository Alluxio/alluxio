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

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Reviews a block allocation, and probably rejects a bad block allocation.
 * The more full a StorageDir, the higher the chance that this reviewer decides
 * NOT to use it for a new block.
 *
 * The intention is to leave a buffer to each {@link StorageDir} by early stopping putting
 * new blocks into it.
 * The existing blocks in the {@link StorageDir} will likely read more data in and expand in size.
 * We want to leave some space for the expansion, to lower the chance of eviction.
 * */
public class ProbabilisticBufferReviewer implements Reviewer {
  private static final Logger LOG = LoggerFactory.getLogger(ProbabilisticBufferReviewer.class);
  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  private final long mHardLimitBytes;
  private final long mSoftLimitBytes;

  /**
   * Constructor the instance from configuration.
   * */
  public ProbabilisticBufferReviewer() {
    InstancedConfiguration conf = ServerConfiguration.global();
    mHardLimitBytes = conf.getBytes(PropertyKey.WORKER_REVIEWER_PROBABILISTIC_HARDLIMIT_BYTES);
    long stopSoftBytes = conf.getBytes(PropertyKey.WORKER_REVIEWER_PROBABILISTIC_SOFTLIMIT_BYTES);
    if (stopSoftBytes <= mHardLimitBytes) {
      LOG.warn("{} should be greater than or equal to {}. Setting {} to {}.",
              PropertyKey.WORKER_REVIEWER_PROBABILISTIC_SOFTLIMIT_BYTES.toString(),
              PropertyKey.WORKER_REVIEWER_PROBABILISTIC_HARDLIMIT_BYTES.toString(),
              PropertyKey.WORKER_REVIEWER_PROBABILISTIC_SOFTLIMIT_BYTES.toString(),
              mHardLimitBytes);
      mSoftLimitBytes = mHardLimitBytes;
    } else {
      mSoftLimitBytes = stopSoftBytes;
    }
  }

  /**
   * Calculates the probability of allowing a new block into this dir,
   * based on how much available space there is.
   * */
  double getProbability(StorageDirView dirView) {
    long availableBytes = dirView.getAvailableBytes();
    long capacityBytes = dirView.getCapacityBytes();

    // Rules:
    // 1. If more than the SOFT limit left, we use this tier. Prob=100%
    // 2. If the tier is less than block size, ignore this tier. Prob=0%
    // 3. If in the middle, the probability is linear to the space left,
    //    the less space the lower.
    if (availableBytes > mSoftLimitBytes) {
      return 1.0;
    }
    if (availableBytes <= mHardLimitBytes) {
      return 0.0;
    }
    // 2 points:
    // Axis X: space usage (commitment)
    // Axis Y: Probability of using this tier
    // (capacity - soft, 1.0)
    // (capacity - hard, 0.0)
    double x = capacityBytes - availableBytes;
    // If HardLimit = SoftLimit, then we would have returned in the previous if-else
    double k = 1.0 / (mHardLimitBytes - mSoftLimitBytes);
    double b = (capacityBytes - mHardLimitBytes + 0.0) / (mSoftLimitBytes - mHardLimitBytes);
    double y = k * x + b;
    LOG.debug("{} bytes available in {}. Probability of staying is {}.", availableBytes,
            dirView.toBlockStoreLocation(), y);
    return y;
  }

  @Override
  public boolean acceptAllocation(StorageDirView dirView) {
    double chance = getProbability(dirView);
    // Throw a dice
    double dice = RANDOM.nextDouble();
    return dice < chance;
  }
}
