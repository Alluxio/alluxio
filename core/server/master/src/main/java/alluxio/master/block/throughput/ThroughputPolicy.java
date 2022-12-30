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
import alluxio.util.CommonUtils;

/**
 * Ufs Read Throughput Policy Interface.
 */
public interface ThroughputPolicy {

  /**
   * The factory for the {@link ThroughputPolicy}.
   */
  class Factory {
    private Factory() {}

    /**
     * Factory for creating {@link ThroughputPolicy}.
     *
     * @param conf Alluxio configuration
     * @return a new instance of {@link ThroughputPolicy}
     */
    public static ThroughputPolicy create(AlluxioConfiguration conf) {
      try {
        Class<ThroughputPolicy> clazz = (Class<ThroughputPolicy>) Class
            .forName(conf.getString(PropertyKey.WORKER_UFS_READ_THROUGHPUT_POLICY));
        return CommonUtils.createNewClassInstance(clazz, new Class[] {AlluxioConfiguration.class},
            new Object[] {conf});
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Calculate worker throughput.
   *
   * @param mWorkers worker info set
   * @param curWorker current worker info
   * @return throughput for current worker
   */
  public long updateWorkerThroughput(IndexedSet<MasterWorkerInfo> mWorkers,
                                     MasterWorkerInfo curWorker);
}
