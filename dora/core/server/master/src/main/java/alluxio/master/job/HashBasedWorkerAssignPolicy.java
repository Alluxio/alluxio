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

package alluxio.master.job;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.dora.ConsistentHashPolicy;
import alluxio.conf.Configuration;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.wire.WorkerInfo;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Policy which employs Hash-Based algorithm to select worker from given workers set.
 */
public class HashBasedWorkerAssignPolicy extends WorkerAssignPolicy {
  ConsistentHashPolicy mWorkerLocationPolicy = new ConsistentHashPolicy(Configuration.global());

  @Override
  protected WorkerInfo pickAWorker(String object, @Nullable Collection<WorkerInfo> workerInfos) {
    if (workerInfos == null) {
      return null;
    }
    List<BlockWorkerInfo> candidates = workerInfos.stream()
        .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
        .collect(Collectors.toList());
    try {
      List<BlockWorkerInfo> blockWorkerInfo = mWorkerLocationPolicy
              .getPreferredWorkers(candidates, object, 1);
      WorkerInfo returnWorker = workerInfos.stream().filter(workerInfo ->
                      workerInfo.getAddress().equals(blockWorkerInfo.get(0).getNetAddress()))
              .findFirst().get();
      return returnWorker;
    } catch (ResourceExhaustedException e) {
      // Tolerate the exception when there is no workers in the cluster
      return null;
    }
  }
}
