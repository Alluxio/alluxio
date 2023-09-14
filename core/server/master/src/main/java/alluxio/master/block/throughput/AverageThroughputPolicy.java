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

package alluxio.master.block.throughput;

import static alluxio.conf.PropertyKey.WORKER_UFS_READ_THROUGHPUT_MAX_MULTIPLE;

import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.block.meta.MasterWorkerInfo;

/**
 * A throughput calculation policy that uses the average value.
 */
public class AverageThroughputPolicy implements ThroughputPolicy {

  private final AlluxioConfiguration mConf;
  private final long mDefaultThroughput;
  private final long mMinThroughput;
  private final long mTotalThroughput;
  private final int mMaxMultiple;

  /**
   * Constructs a new {@link ConstantThroughputPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public AverageThroughputPolicy(AlluxioConfiguration conf) {
    mConf = conf;
    mDefaultThroughput = mConf.getBytes(PropertyKey.WORKER_UFS_READ_DEFAULT_THROUGHPUT);
    mMinThroughput = mConf.getBytes(PropertyKey.WORKER_UFS_READ_MIN_THROUGHPUT);
    mTotalThroughput = mConf.getBytes(PropertyKey.WORKER_UFS_READ_TOTAL_THROUGHPUT);
    mMaxMultiple = mConf.getInt(WORKER_UFS_READ_THROUGHPUT_MAX_MULTIPLE);
  }

  @Override
  public long updateWorkerThroughput(IndexedSet<MasterWorkerInfo> workers,
      MasterWorkerInfo curWorker) {
    long workingCount = workers.stream().filter(workerInfo ->
        workerInfo.getReadUfsThroughput() != 0 || workerInfo == curWorker).count();
    // curWorker has been lost
    if (workingCount == 0) {
      return mDefaultThroughput;
    }
    long averageThroughput = mTotalThroughput / workingCount;
    long maxThroughput = mTotalThroughput / workers.size()
        * Math.min(workers.size(), mMaxMultiple);
    return Math.max(mMinThroughput, Math.min(averageThroughput, maxThroughput));
  }
}
