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

import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.block.meta.MasterWorkerInfo;

/**
 * A throughput calculation policy that uses the constant value.
 */
public class ConstantThroughputPolicy implements ThroughputPolicy {

  private final AlluxioConfiguration mConf;
  private final long mThroughput;

  /**
   * Constructs a new {@link ConstantThroughputPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public ConstantThroughputPolicy(AlluxioConfiguration conf) {
    mConf = conf;
    mThroughput = mConf.getBytes(PropertyKey.WORKER_UFS_READ_DEFAULT_THROUGHPUT);
  }

  @Override
  public long updateWorkerThroughput(IndexedSet<MasterWorkerInfo> mWorkers,
      MasterWorkerInfo curWorker) {
    return mThroughput;
  }
}
