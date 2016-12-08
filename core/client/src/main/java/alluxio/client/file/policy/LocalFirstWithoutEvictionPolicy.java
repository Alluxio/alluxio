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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
/**
 * A policy that returns local host first, and if the local worker doesn't have enough availability,
 * it randomly picks a worker from the active workers list for each block write.
 * If No worker meet the demands, return local host.
 */
@ThreadSafe
public class LocalFirstWithoutEvictionPolicy implements FileWriteLocationPolicy {
  private String mLocalHostName;
  /**
   * Constructs a {@link LocalFirstWithoutEvictionPolicy}.
   */
  public LocalFirstWithoutEvictionPolicy() {
    mLocalHostName = NetworkAddressUtils.getLocalHostName();
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    // try the local host first
    WorkerNetAddress localWorkerNetAddress = null;
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(mLocalHostName)) {
        localWorkerNetAddress = workerInfo.getNetAddress();
        if (workerInfo.getAvailableBytes() >= blockSizeBytes) {
          return localWorkerNetAddress;
        }
      }
    }

    // otherwise randomly pick a worker that has enough availability
    List<BlockWorkerInfo> shuffledWorkers = Lists.newArrayList(workerInfoList);
    Collections.shuffle(shuffledWorkers);
    for (BlockWorkerInfo workerInfo : shuffledWorkers) {
      if (workerInfo.getAvailableBytes() >= blockSizeBytes) {
        return workerInfo.getNetAddress();
      }
    }
    return localWorkerNetAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalFirstWithoutEvictionPolicy)) {
      return false;
    }
    LocalFirstWithoutEvictionPolicy that = (LocalFirstWithoutEvictionPolicy) o;
    return Objects.equal(mLocalHostName, that.mLocalHostName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLocalHostName);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("localHostName", mLocalHostName)
        .toString();
  }
}
