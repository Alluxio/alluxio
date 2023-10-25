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

import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.wire.WorkerInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Policy which employs Round Robin around given workers to select worker.
 */
public class RoundRobinWorkerAssignPolicy implements WorkerAssignPolicy {
  private AtomicInteger mCounter = new AtomicInteger(0);

  @Override
  public List<WorkerInfo> pickWorkers(String object, Set<WorkerInfo> workerInfos,
      int count) {
    if (count > workerInfos.size()) {
      throw new ResourceExhaustedRuntimeException(
          String.format("Not enough workers to pick from, expected %d, got %d", count,
              workerInfos.size()), false);
    }
    ArrayList workers = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int nextWorkerIdx = Math.floorMod(mCounter.incrementAndGet(), workerInfos.size());
      WorkerInfo pickedWorker =
          workerInfos.toArray(new WorkerInfo[workerInfos.size()])[nextWorkerIdx];
      workers.add(pickedWorker);
    }
    return workers;
  }
}
