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

import alluxio.wire.WorkerInfo;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Policy which employs Round Robin around given workers to select worker.
 */
public class RoundRobinWorkerAssignPolicy extends WorkerAssignPolicy {
  private AtomicInteger mCounter = new AtomicInteger(0);

  @Override
  protected WorkerInfo pickAWorker(String object, Collection<WorkerInfo> workerInfos) {
    if (workerInfos.isEmpty()) {
      return null;
    }
    int nextWorkerIdx = Math.floorMod(mCounter.incrementAndGet(), workerInfos.size());
    WorkerInfo pickedWorker = workerInfos.toArray(new WorkerInfo[workerInfos.size()])
        [nextWorkerIdx];
    return pickedWorker;
  }
}
