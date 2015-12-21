/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file.policy;

import java.util.Collections;
import java.util.List;

import tachyon.client.ClientContext;
import tachyon.client.block.BlockWorkerInfo;
import tachyon.util.network.NetworkAddressUtils;

/**
 * A policy that returns local host first, and if the local worker doesn't have enough availability,
 * it randomly picks a worker from the active workers list.
 */
public final class LocalFirstPolicy implements FileWriteLocationPolicy<LocalFirstPolicyOptions> {
  private String mLocalHostName = null;

  @Override
  public String getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList, long blockSizeBytes) {
    // first try the local host
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getHost().equals(mLocalHostName)
          && workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() > blockSizeBytes) {
        return mLocalHostName;
      }
    }

    // otherwise randomly pick a worker that has enough availability
    Collections.shuffle(workerInfoList);
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() > blockSizeBytes) {
        return workerInfo.getHost();
      }
    }
    return null;
  }

  @Override
  public void initialize(List<BlockWorkerInfo> workerInfoList,
      LocalFirstPolicyOptions policyOptions) {
    mLocalHostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
  }
}
