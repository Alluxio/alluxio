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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A policy that returns the worker with the most available bytes. The policy returns null if no
 * worker is qualified.
 */
@ThreadSafe
public final class MostAvailableFirstPolicy implements FileWriteLocationPolicy {

  @Override
  public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    List<BlockWorkerInfo> inputList = Lists.newArrayList(workerInfoList);
    long mostAvailableBytes = -1;
    WorkerNetAddress result = null;
    for (BlockWorkerInfo workerInfo : inputList) {
      if (workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() > mostAvailableBytes) {
        mostAvailableBytes = workerInfo.getCapacityBytes() - workerInfo.getUsedBytes();
        result = workerInfo.getNetAddress();
      }
    }
    return result;
  }
}
