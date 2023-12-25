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
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.membership.WorkerClusterView;
import alluxio.wire.WorkerInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Policy which employs Hash-Based algorithm to select worker from given workers set.
 */
public class HashBasedWorkerAssignPolicy implements WorkerAssignPolicy {
  ConsistentHashPolicy mWorkerLocationPolicy = new ConsistentHashPolicy(Configuration.global());

  @Override
  public List<WorkerInfo> pickWorkers(String object, @Nullable Set<WorkerInfo> workerInfos,
      int count) {
    if (workerInfos == null) {
      return Collections.emptyList();
    }

    WorkerClusterView candidates = new WorkerClusterView(workerInfos);
    try {
      List<BlockWorkerInfo> blockWorkerInfo = mWorkerLocationPolicy
          .getPreferredWorkers(candidates, object, count);
      ArrayList workers = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        int finalI = i;
        WorkerInfo returnWorker = workerInfos.stream().filter(workerInfo ->
                workerInfo.getIdentity().equals(blockWorkerInfo.get(finalI).getIdentity()))
            .findFirst().get();
        workers.add(returnWorker);
      }
      return workers;
    } catch (ResourceExhaustedException e) {
      throw new ResourceExhaustedRuntimeException(e.getMessage(), e, false);
    }
  }
}
