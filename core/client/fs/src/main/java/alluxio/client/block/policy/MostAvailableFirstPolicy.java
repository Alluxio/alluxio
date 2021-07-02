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

package alluxio.client.block.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A policy that returns the worker with the most available bytes.
 */
@ThreadSafe
public final class MostAvailableFirstPolicy implements BlockLocationPolicy {

  /**
   * Constructs a new {@link MostAvailableFirstPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public MostAvailableFirstPolicy(AlluxioConfiguration conf) {}

  /**
   * The policy returns null if no worker is qualified.
   */
  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    long mostAvailableBytes = -1;
    WorkerNetAddress result = null;
    for (BlockWorkerInfo workerInfo : options.getBlockWorkerInfos()) {
      if (workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() > mostAvailableBytes) {
        mostAvailableBytes = workerInfo.getCapacityBytes() - workerInfo.getUsedBytes();
        result = workerInfo.getNetAddress();
      }
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof MostAvailableFirstPolicy;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }
}
