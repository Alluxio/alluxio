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
