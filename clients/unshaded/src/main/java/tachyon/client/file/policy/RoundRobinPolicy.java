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

import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.client.block.BlockWorkerInfo;
import tachyon.client.file.options.OutStreamOptions;

/**
 * A policy that chooses the worker for the next block in a RR manner and skips workers that do not
 * have enough space. The policy returns null if no worker can be found.
 */
public final class RoundRobinPolicy implements FileWriteLocationPolicy<RoundRobinPolicyOptions> {
  private final OutStreamOptions mOptions;
  private final List<BlockWorkerInfo> mWorkerInfoList;
  private int mIndex = 0;

  /**
   * Creates the policy.
   */
  public RoundRobinPolicy(List<BlockWorkerInfo> workerInfoList, OutStreamOptions options) {
    mOptions = Preconditions.checkNotNull(options);
    mWorkerInfoList = Preconditions.checkNotNull(workerInfoList);
  }

  @Override
  public String getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList) {
    // at most try all the workers
    for (int i = 0; i < mWorkerInfoList.size(); i ++) {
      String candidate = mWorkerInfoList.get(mIndex).getHost();
      BlockWorkerInfo workerInfo = findBlockWorkerInfo(workerInfoList, candidate);
      if (workerInfo == null || workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() < mOptions
          .getBlockSizeBytes()) {
        continue;
      }
      mIndex = (mIndex + 1) % mWorkerInfoList.size();
      return candidate;
    }
    return null;
  }

  /**
   * @param workerInfoList the list of worker info
   * @param hostname the hostname to look for
   * @return the worker info in the list that matches the host name, null if not found
   */
  private BlockWorkerInfo findBlockWorkerInfo(List<BlockWorkerInfo> workerInfoList,
      String hostname) {
    for (BlockWorkerInfo info : workerInfoList) {
      if (info.getHost().equals(hostname)) {
        return info;
      }
    }
    return null;
  }
}
