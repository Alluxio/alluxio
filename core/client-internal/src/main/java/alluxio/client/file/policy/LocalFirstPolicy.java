/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.policy;

import alluxio.client.ClientContext;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
/**
 * A policy that returns local host first, and if the local worker doesn't have enough availability,
 * it randomly picks a worker from the active workers list for each block write.
 */
@ThreadSafe
public final class LocalFirstPolicy implements FileWriteLocationPolicy {
  private String mLocalHostName = null;

  /**
   * Constructs a {@link LocalFirstPolicy}.
   */
  public LocalFirstPolicy() {
    mLocalHostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    // first try the local host
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(mLocalHostName)
          && workerInfo.getCapacityBytes() >= blockSizeBytes) {
        return workerInfo.getNetAddress();
      }
    }

    // otherwise randomly pick a worker that has enough availability
    Collections.shuffle(workerInfoList);
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getCapacityBytes() >= blockSizeBytes) {
        return workerInfo.getNetAddress();
      }
    }
    return null;
  }
}
